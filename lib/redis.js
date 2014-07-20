module.exports = function () {
    var util   = require('util'),
        redis  = require('redis');

    function Redis(config) {
        var $this = this;
        this.config = {};

        this.config.redis_urls = process.env.REDIS_URLS || config.redis_urls;
        this.meta = config.meta;
        this.logger = config.logger;

        this.logger.log('debug', 'redis::urls=', this.config.redis_urls,this.meta);
        try {
            this.servers = this.config.redis_urls.split(',').map(function (url) {
                var tuple = url.split(':');
                return {
                    host  : tuple[0],
                    port  : tuple[1],
                    state : 'url-parsed',
                    ready : false,
                    url   : url,
                    ok    : 0,
                    fail  : 0,
                    last  : 'none',
                    disconnections: 0
                };
            });
            this.logger.log('debug', 'redis::servers=', this.servers, this.meta);
        } catch (e) {
            this.exception = e;
        }
    }

    Redis.prototype.connect = function (next) {
        var $this = this;
        if (this.exception || !this.servers || this.servers.length < 1) {
            return -1;
        }

        var transaction = new Transaction('redis::connect()', this.servers.length, function (errors, values, metas) { // majority
            $this.logger.log(transaction.isMajority() ? 'info' : 'error', "redis(%s)::Transaction(%s)|Majority|errors=%j|value=%j|metas=%j", $this.config.redis_urls, 'redis::connect()', errors, values, metas, $this.meta);
        }, function (errors, values, metas) { // all
            $this.logger.log(transaction.isTotalSuccess() ? 'debug' : (transaction.isMajority() ? 'warn' : 'error'), "redis(%s)::Transaction(%s)|%s|errors=%j|values=%j|metas=%j",
                $this.config.redis_urls,
                'redis::connect()',
                transaction.isTotalSuccess() ? 'ALL-Servers' : (transaction.isMajority() ? 'Majority-Servers' : 'Failed'),
                errors,
                values,
                metas,
                $this.meta);
            var majority = transaction.isMajority();
            transaction = null;
            next(majority ? null : errors, values);
        });

        function _connect(server) {
            server.state = 'creating-client';
            $this.logger.log('info', "redis(%s)::connect(%s:%s)|state[%s]", server.url, server.host, server.port, server.state, $this.meta);
            server.redis = redis.createClient(server.port, server.host);
            server.redis.on("connect", function () {
                $this.logger.log('info', "redis(%s)::on(connect)|state[%s->connect]", server.url, server.state, $this.meta);
                server.state = "connect";
                server.ready = false;
            });
            server.redis.on("ready", function () {
                $this.logger.log('debug', "redis(%s)::on(ready)|state[%s->ready]", server.url, server.state, $this.meta);
                server.state = "ready";
                server.ready = true;
                if (transaction)
                    transaction.succeeded(null, "ready", server.url);
            });
            server.redis.on("error", function (err) {
                $this.logger.log('error', "redis(%s)::on(error)|state[%s->error]|err=%s", server.url, server.state, util.inspect(err, true), $this.meta);
                server.state = "error";
                server.fail += 1;
                if (err.message.indexOf("ECONNREFUSED") < 0) {
                    $this.logger.log('error', "*********** REDIS(%s) *************: the error was not a connection closed and the driver is usually stuck. Restart of redisd maybe required", server.url);
                    process.exit(-1);
                }
                if (transaction)
                    transaction.failed(err, null, server.url);
            });
            server.redis.on("end", function () {
                $this.logger.log('error', "redis(%s)::on(end)|state[%s->end]| ********************** CONNECTION LOST WITH REDIS %s **********************", server.url, server.state, server.url, $this.meta);
                server.state = "end";
                server.ready = false;
                server.disconnections += 1;
                if (transaction)
                    transaction.failed(new Error("closed"), null, server.url);
            });
            server.redis.on("drain", function () {
                $this.logger.log('debug', "redis(%s)::on(drain)|state[%s->drain]", server.url, server.state, $this.meta);
                server.state = "drain";
            });
            server.redis.on("idle", function () {
                $this.logger.log('debug', "redis(%s)::on(idle)|state[%s->idle]", server.url, server.state, $this.meta);
                server.state = "idle";
            });
        }
        // Connecting to all the redis servers
        this.servers.forEach(function (server) {
            process.nextTick(function () {
                _connect(server);
            });
        });
        return 0;
    }

    Redis.prototype.close = function () {
        // Disconnect to all the redis servers
        this.servers.forEach(function (server) {
            process.nextTick(function () {
                server.redis.end();
            });
        });
    }

    Redis.prototype._majority = function(logMsg, command, next) {
        var $this = this;
        next = next || $this.redis.print;

        var transaction = new Transaction(logMsg, this.servers.length, function (errors, values, metas) { // majority
            $this.logger.log(transaction.isMajority() ? 'debug' : 'error', "redis(%s)::Transaction(%s)|Majority|errors=%j|values=%j|metas=%j", $this.config.redis_urls, logMsg, errors, values, metas, $this.meta);
            next(transaction.isMajority() ? null : errors, values.filter(function (row) {
                return row ? true : false;
            })[0], metas.filter(function (row) {
                return row ? true : false;
            })[0]);
        }, function (errors, values, metas) { // all
            $this.logger.log(transaction.isTotalSuccess() ? 'debug' : (transaction.isMajority() ? 'warn' : 'error'), "redis(%s)::Transaction(%s)|%s|errors=%j|values=%j|metas=%j",
                $this.config.redis_urls,
                logMsg,
                transaction.isTotalSuccess() ? 'ALL-Servers' : (transaction.isMajority() ? 'Majority-Servers' : 'Failed'),
                errors,
                values,
                metas,
                $this.meta);
        });

        this.servers.forEach(function (server) {
            process.nextTick(function () {
                command(server, function (err, value) {
                    $this.logger.log('debug', "redis(%s)::%s|err=%j|value=%j", $this.config.redis_urls, logMsg, err, value, $this.meta);
                    if (err) {
                        server.fail += 1;
                        transaction.failed(err, value, server.url);
                    } else {
                        server.ok += 1;
                        transaction.succeeded(err, value, server.url);
                    }
                });
            });
        });
    };

    Redis.prototype._first = function(logMsg, command, next) {
        var $this = this;
        next = next || $this.redis.print;

        function tryServer(index) {
            // No more server to try?
            if (index >= $this.servers.length) {
                $this.logger.log('debug', "redis(%s)::%s|FAILED|No server responded", $this.config.redis_urls, logMsg, $this.meta);
                return next(new Error("redis::" + logMsg + "|FAILED"));
            }

            // Try this one
            var server = $this.servers[index];
            if (server.ready) {
                server.last = logMsg;
                return command(server, function (err, values) {
                    $this.logger.log('debug', "redis(%s)::%s|err=%j|values=%j", $this.config.redis_urls, logMsg, err, values, $this.meta);
                    if (err) {
                        server.fail += 1;
                        tryServer(index +1);
                    } else {
                        server.ok += 1;
                        next(err, values, server);
                    }
                });
            }

            // Server not ready?
            $this.logger.log('debug', "redis(%s/%s/%s)::%s|trying next server.", server.url, server.state, (server.ready ? "ready" : "not-ready"), logMsg, $this.meta);
            return tryServer(index +1);
        }

        tryServer(0);
    };





    Redis.prototype.get = function(key, next) {
        this._first('get(key=' + key + ')', function (server, callback) {
            server.redis.get(key, callback);
        }, next);
    };

    Redis.prototype.keys = function (pattern, next) {
        this._first('keys(pattern=' + pattern + ')', function (server, callback) {
            server.redis.keys(pattern, callback);
        }, next);
    };

    Redis.prototype.mget = function (keys, next) {
        this._first('mget(keys=' + keys + ')', function (server, callback) {
            server.redis.mget(keys, callback);
        }, next);
    };

    Redis.prototype.mgetkeys = function (pattern, next) {
        var $this = this;

        $this._first('mgetkeys(pattern=' + pattern + ')', function (server, callback) {
            server.redis.keys(pattern, callback);
        }, function (err, keys, serverKeys) {
            if (err)
                return next(err);
            $this._first('mgetkeys(pattern=' + pattern + ', keys=' +  keys + ')', function (server, callback) {
                server.redis.mget(keys, callback);
            }, function (err, values, serverValues) {
                if (err)
                    return next(err);
                var object = {};
                for(var i = 0 ; i < keys.length && i < values.length ; ++i) {
                    object[keys[i]] = values[i];
                }
                next(null, object, [serverKeys, serverValues]);
            });
        });
    };

    Redis.prototype.set = function (key, value, next) {
        this._majority('set(key=' + key + ', value=' + value + ')', function (server, callback) {
            server.redis.set(key, value, callback);
        }, next);
    };

    Redis.prototype.del = function (key, next) {
        this._majority('del(key=' + key + ')', function (server, callback) {
            server.redis.del(key, callback);
        }, next);
    };

    Redis.prototype.status = function (next) {
        if (next) {
            next(null, this.servers.map(function (server) {
                return {url: server.url, state: server.state, ready: server.ready, ok: server.ok, fail: server.fail, last: server.last};
            }));
        }
    };



    function Transaction(id, serverCount, onCompleted, onTeminated) {
        this.id = id;
        this.count = serverCount;
        this.majority = Math.floor(this.count /2) +1;
        this.stats = {
            succeeded : 0,
            failed    : 0
        };
        this.completed   = false;  // Majority of completed
        this.terminated  = false;  // All servers completed
        this.onCompleted = onCompleted;
        this.onTeminated = onTeminated;
        this.values = [];
        this.errors = [];
        this.metas  = [];
    }

    Transaction.prototype.succeeded = function (err, value, meta) {
        this.stats.succeeded += 1;
        this.errors.push(err);
        this.values.push(value);
        this.metas.push(meta);

        if (this.stats.succeeded === this.majority) {
            this.completed = true;
            this.onCompleted(this.errors, this.values, this.metas);
        }
        if (this.stats.succeeded + this.stats.failed === this.count) {
            this.terminated = true;
            this.onTeminated(this.errors, this.values, this.metas);
        }
    };

    Transaction.prototype.failed = function (err, value, meta) {
        this.stats.failed += 1;
        this.errors.push(err);
        this.values.push(value);
        this.metas.push(meta);
        if (this.stats.failed === this.majority) {
            this.completed = true;
            this.onCompleted(this.errors, this.values, this.metas);
        }
        if (this.stats.succeeded + this.stats.failed === this.count) {
            this.terminated = true;
            this.onTeminated(this.errors, this.values, this.metas);
        }
    };
    Transaction.prototype.isMajority = function () {
        return (this.stats.succeeded >= this.majority);
    };

    Transaction.prototype.isTotalSuccess = function () {
        return (this.stats.succeeded === this.count);
    };


    function create(config) {
        return new Redis(config);
    }

    return {
        create : create
    };
}();
