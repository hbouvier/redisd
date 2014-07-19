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

    Redis.prototype.connect = function () {
        var $this = this;
        if (this.exception || !this.servers || this.servers.length < 1) {
            return -1;
        }

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
            });
            server.redis.on("error", function (err) {
                $this.logger.log('error', "redis(%s)::on(error)|state[%s->error]|err=%s", server.url, server.state, util.inspect(err, true), $this.meta);
                server.state = "error";
                server.fail += 1;
                if (err.message.indexOf("ECONNREFUSED") < 0) {
                    $this.logger.log('error', "*********** REDIS(%s) *************: the error was not a connection closed and the driver is usually stuck. Restart of redisd maybe required", server.url);
                }
            });
            server.redis.on("end", function () {
                $this.logger.log('error', "redis(%s)::on(end)|state[%s->end]| ********************** CONNECTION LOST WITH REDIS %s **********************", server.url, server.state, server.url, $this.meta);
                server.state = "end";
                server.ready = false;
                server.disconnections += 1;
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

    Redis.prototype._all = function(logMsg, command, next) {
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
        this._all('get(key=' + key + ')', function (server, callback) {
            server.redis.get(key, callback);
        }, next);
    };

    Redis.prototype.keys = function (pattern, next) {
        this._all('keys(pattern=' + pattern + ')', function (server, callback) {
            server.redis.keys(pattern, callback);
        }, next);
    };

    Redis.prototype.mget = function (keys, next) {
        this._all('mget(keys=' + keys + ')', function (server, callback) {
            server.redis.mget(keys, callback);
        }, next);
    };

    Redis.prototype.mgetkeys = function (pattern, next) {
        var $this = this;

        $this._all('mgetkeys(pattern=' + pattern + ')', function (server, callback) {
            server.redis.keys(pattern, callback);
        }, function (err, keys, serverKeys) {
            if (err)
                return next(err);
            console.log('keys:', keys);
            $this._all('mgetkeys(pattern=' + pattern + ', keys=' +  keys + ')', function (server, callback) {
                server.redis.mget(keys, callback);
            }, function (err, values, serverValues) {
                if (err)
                    return next(err);
                console.log('values:', values);
                var vector = [];
                for(var i = 0 ; i < keys.length && i < values.length ; ++i) {
                    vector.push({key:keys[i],value:values[i]});
                }
                next(null, vector, [serverKeys, serverValues]);
            });
        });
    };

    Redis.prototype.set = function (key, value, next) {
        this._all('set(key=' + key + ', value=' + value + ')', function (server, callback) {
            server.redis.set(key, value, callback);
        }, next);
    };

    Redis.prototype.del = function (key, next) {
        this._all('del(key=' + key + ')', function (server, callback) {
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

    function create(config) {
        return new Redis(config);
    }

    return {
        create : create
    };
}();
