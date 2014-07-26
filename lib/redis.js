module.exports = function () {
    var util   = require('util'),
        redis  = require('redis'),
        Transaction = require('./transaction');

    function Redis(config) {
        var $this = this;
        this.config = {};

        this.config.redis_urls = process.env.REDIS_URLS || config.redis_urls;
        this.meta = config.meta;
        this.logger = config.logger;
        this.publisher = null;

        this.logger.log('debug', 'redis::urls=', this.config.redis_urls, this.meta);
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

        var transaction = new Transaction('redis::connect()', this.servers, function (server, callback) {
                _connect(server);
            }, this.logger);

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
                    transaction.succeeded(null, "ready", server);
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
                    transaction.failed(err, null, server);
            });
            server.redis.on("end", function () {
                $this.logger.log('error', "redis(%s)::on(end)|state[%s->end]| ********************** CONNECTION LOST WITH REDIS %s **********************", server.url, server.state, server.url, $this.meta);
                server.state = "end";
                server.ready = false;
                server.disconnections += 1;
                if (transaction)
                    transaction.failed(new Error("closed"), null, server);
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
        transaction.all(function (trx) {
            transaction = null; // To avoid the _connect callback to be invoked after completion
            next(trx.hasAllSucceeded() ? null : trx.errors, trx.values, trx.metas);
        }, this.logger);
        return 0;
    }

    Redis.prototype.close = function () {
        var $this = this;
        // Disconnect to all the redis servers
        var transaction = new Transaction('redis::close()', this.servers, function (server, callback) {
                server.redis.end();
            }, this.logger);

        transaction.all(function (trx) {
            $this.logger.log('debug', "redis::close() %s", trx.hasAllSucceeded() ? "success" : "failed", $this.meta);
        }, this.logger);

    }
/*
    Redis.prototype.majority = function(id, command, next) {
        var $this = this;
        next = next || $this.servers[0].redis.subscribe;

        var transaction = new Transaction(id, this.servers.length,
                null, // one
                function (errors, values, metas) { // majority
                    // Return only the first result and NO errors
                    // if we have reached the majority
                    // TODO: Validate that all results are CONSISTENT!!!
                    next(transaction.isMajority() ? null : errors,
                        values.filter(function (row) {
                            return row ? true : false;
                        })[0],
                        metas.filter(function (row) {
                            return row ? true : false;
                        })[0]);
                },
                null, // all
                this.logger);

        this.servers.forEach(function (server) {
            process.nextTick(function () {
                command(server, function (err, value) {
                    if (err) {
                        server.fail += 1;
                        transaction.failed(err, value, { url : server.url });
                    } else {
                        server.ok += 1;
                        transaction.succeeded(err, value, { url : server.url });
                    }
                });
            });
        });
    };

    Redis.prototype.first = function(id, command, next) {
        var $this = this;
        next = next || $this.servers[0].redis.print;

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
    */

    function flatnext(trx, next) {
        if (trx.hasSucceeded()) {
            var result = null;
            var values = trx.values.filter(function (oneValue) {
                return oneValue ? true : false;
            });
            var metas = trx.metas.filter(function (oneMeta) {
                return oneMeta ? true : false;
            });
            return next (null, values[0], metas[0]);
        }
        next(trx.errors, trx.values, trx.metas);
    }

    Redis.prototype.get = function(key, next) {
        var $this = this,
            transaction = new Transaction('get(key=' + key + ')', this.servers, function (server, callback) {
                server.redis.get(key, callback);
            }, this.logger);

        transaction.first(function (trx) {
            flatnext(trx, next);
        });
    };

    Redis.prototype.keys = function (pattern, next) {
        var $this = this,
            transaction = new Transaction('keys(pattern=' + pattern + ')', this.servers, function (server, callback) {
                server.redis.keys(pattern, callback);
            }, this.logger);

        transaction.first(function (trx) {
            flatnext(trx, next);
        });
    };

    Redis.prototype.mget = function (keys, next) {
        var $this = this,
            transaction = new Transaction('mget(keys=' + keys + ')', this.servers, function (server, callback) {
                server.redis.mget(keys, callback);
            }, this.logger);

        transaction.first(function (trx) {
            flatnext(trx, next);
        });
    };

    function wrap(msg, next, logger, meta) {
       return function (err, value) {
          try {
            logger.log('debug', 'CALLBACK: IN:  %s, err:%j, value:%j', msg, err, value, meta);
            next(err, value);
            logger.log('debug', 'CALLBACK: OUT: %s, err:%j, value:%j', msg, err, value, meta);
          } catch (e) {
              logger.log('error', 'CALLBACK: OUT: ************* %s, err:%j, value:%j [EXCEPTION: %s] *************', msg, err, value, e, meta);
          }
       };
    }

    Redis.prototype.mgetkeys = function (pattern, next) {
        var $this = this,
            transactionKeys = new Transaction('mgetkeys(pattern=' + pattern + ')', this.servers, function (server, callback) {
                server.redis.keys(pattern, wrap('mgetkeys(pattern=' + pattern + ')', callback, $this.logger, $this.meta));
            }, this.logger);

        transactionKeys.first(function (trxKeys) {
            flatnext(trxKeys, function(err, keys, meta) {
                if (err)
                    return next(err);

                var transactionValues = new Transaction('mgetkeys(pattern=' + pattern + ', keys=' +  keys + ')', $this.servers, function (server, callback) {
                    server.redis.mget(keys, wrap('mgetkeys(pattern=' + pattern + ', keys=' +  keys + ')', callback, $this.logger, $this.meta));
                }, $this.logger);

                transactionValues.first(function (trxValues) {
                    flatnext(trxValues, function(err, values, meta) {
                        if (err)
                            return next(err);
                        var object = {};
                        for(var i = 0 ; i < keys.length && i < values.length ; ++i) {
                            object[keys[i]] = values[i];
                        }
                        next(null, object, [trxKeys.meta, trxValues.meta]);
                    });
                });
            });
        });
    };

    Redis.prototype.publish = function (channel, message) {
        var $this = this;
        var pubTransaction = new Transaction('publish(channel=' + channel +', message=' + message + ')', this.servers, function (server, callback) {
            server.redis.publish(channel, message);
            callback(null, "OK");
        }, this.logger);

        pubTransaction.majority(function (pubTrx) {
            $this.logger.log('debug', 'redis::publish(channel=%s, message=%s)|%s', channel, message,
                             (pubTrx.hasSucceeded() ? "success" : "failed"), $this.meta);
        });
    };

    Redis.prototype.set = function (key, value, next) {
        var $this = this,
            transaction = new Transaction('set(key=' + key + ', value=' + value + ')', this.servers, function (server, callback) {
                server.redis.set(key, value, callback);
            }, this.logger);

        transaction.majority(function (trx) {
            if (trx.hasSucceeded()) {
                $this.publish('redis:set', JSON.stringify({key:key, value:value}));
            }
            flatnext(trx, next);
        });
    };

    Redis.prototype.del = function (key, next) {
        var $this = this,
            transaction = new Transaction('del(key=' + key + ')', this.servers, function (server, callback) {
                server.redis.del(key, callback);
            }, this.logger);

        transaction.majority(function (trx) {
            if (trx.hasSucceeded()) {
                $this.publish('redis:del', JSON.stringify({key:key}));
            }
            flatnext(trx, next);
        });
    };

    Redis.prototype.status = function (next) {
        if (next) {
            next(null, this.servers.map(function (server) {
                return {url: server.url, state: server.state, ready: server.ready, ok: server.ok, fail: server.fail, last: server.last};
            }));
        }
    };

    Redis.prototype.subscribe = function (channel) {
        var $this = this,
            transaction = new Transaction('subscribe(channel=' + channel + ')', this.servers, function (server, callback) {
                server.redis.subscribe(channel);
                callback(null, "OK");
            }, this.logger);

        transaction.majority(function (trx) {
            flatnext(trx, function(err, result, meta){});
        });
    };

    Redis.prototype.on = function (channel, next) {
        this.servers.forEach(function (server) {
            server.redis.on(channel, function (aChannel, message) {
                next(aChannel, message, server);
            });
        });
    };

    Redis.prototype.setPublisher = function (publisher) {
        this.publisher = publisher;
    };
    Redis.prototype.getPublisher = function () {
        return this.publisher;
    };



    function create(config) {
        return new Redis(config);
    }

    return {
        create : create
    };
}();
