module.exports = function () {

    function routes(app, config, io) {
        var redis  = config.redis;

        redis.getPublisher().subscribe('redis:set');
        redis.getPublisher().subscribe('redis:del');
        redis.getPublisher().on("message", function (channel, message) {
            console.log('channel:', channel, ', message:', message);
            config.logger.log('info', 'sub(channel:%s,message:%s)', channel, message, config.meta);
            switch(channel) {
                case 'redis:set':
                    io.sockets.emit('redis:set', message);
                    break;
                case 'redis:del':
                    io.sockets.emit('redis:del', message);
                    break;
                default:
                    break;
            }
        });

        // GET
        app.get('/redis/api/v1/key/:key?', function (req, res) {
            res.header('X-redis-http-method', 'GET');
            res.header('X-redis-operation', 'get');
            if (req.params.key) {
                res.header('X-redis-multi-key', false);
                res.header('X-redis-key', req.params.key);
                redis.get(req.params.key, function (err, value, server) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,error:err}).end();
                    }
                    res.header('X-redis-server', server.url);
                    if (value) {
                        return res.set('Content-Type', 'text/plain; charset=utf8').send(value).end();
                    }
                    return res.status(404).json({key:req.params.key}).end();
                });
            } else {
                res.header('X-redis-multi-key', true);
                res.header('X-redis-key', '*');
                redis.mgetkeys("*", function (err, values, servers) {
                    if (err) {
                        return res.status(500).json({key:'*',error:err}).end();
                    }
                    res.header('X-redis-server', servers[0].url +'/'+ servers[1].url);
                    if (values) {
                        return res.set('Content-Type', 'text/plain; charset=utf8').send(values).end();
                    }
                    return res.status(404).json({key:'*'}).end();
                });
            }
        });

        function getFormData(defaultKey, req) {
            if (req.xContentType === 'application/x-www-form-urlencoded') {
                var tuples = [];
                for (var name in req.body) {
                    if (req.body.hasOwnProperty(name)) {
                        tuples.push({key: name, value: req.body[name]});
                    }
                }
                // Invalid Post using something like curl
                if (tuples.length === 1 && !tuples[0].value) {
                    tuples[0].value = tuples[0].key;
                    tuples[0].key   = defaultKey;
                }
                return tuples;
            }
            return null;
        }

        // POST
        app.post('/redis/api/v1/key', function (req, res) {
            var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });
            res.header('X-redis-http-method', 'POST');
            res.header('X-redis-operation', 'set');

            if (req.xContentType === 'application/x-www-form-urlencoded') {
                var tuples  = getFormData(uuid, req);
                var total   = 0,
                    failed  = 0,
                    errors  = [],
                    servers = {};
                function _set(list, server_url) {
                    if (!list || list.length < 1) {
                        res.header('X-redis-server', Object.keys(servers).join(','));
                        if (tuples.length === 1) {
                            res.header('X-redis-multi-key', false);
                            res.header('X-redis-key', tuples[0].key);
                            return res.status(failed === 0 ? 204  : 500).end();
                        } else {
                            res.header('X-redis-multi-key', true);
                            res.header('X-redis-key', tuples.map(function (object) {return object.key}).join(','));
                            if (failed === 0) {
                                return res.status(204).end();
                            } else {
                                return res.status(failed < total ? 206 : 500).json({
                                    keys: tuples.map(function (obj) {
                                        return obj.key;
                                    }),
                                    value: tuples.map(function (obj) {
                                        return obj.value;
                                    }),
                                    errors: errors,
                                    total: total,
                                    failed: failed
                                }).end();
                            }
                        }
                    }
                    var obj = list.shift();
                    redis.set(obj.key, obj.value, function (err, result, serverURL) {
                        errors.push(err);
                        if (err) {
                            failed += 1;
                        } else {
                            total += 1;
                            servers[serverURL] = true;
                            // io.sockets.emit('new:key', {key: obj.key, value: obj.value, server: serverURL});
                        }
                        process.nextTick(function () {
                            _set(list, serverURL);
                        });
                    });
                }
                _set(tuples.slice(0));
            } else {
                var key    = uuid,
                    value  = req.body;
                res.header('X-redis-multi-key', false);
                res.header('X-redis-key', key);

                redis.set(key, value, function (err, result, serverURL) {
                    if (err) {
                        return res.status(500).json({key:key,error:err}).end();
                    }
                    res.header('X-redis-server', serverURL);
                    // io.sockets.emit('new:key', {key:key, value: value, server:serverURL});
                    res.status(204).end();
                });
            }
        });

        // PUT
        app.put('/redis/api/v1/key/:key', function (req, res) {
            var key   = req.params.key,
                value = req.body;

            res.header('X-redis-http-method', 'PUT');
            res.header('X-redis-operation', 'set');
            var tuples = getFormData(key, req);
            if (tuples) {
                if (!req.body && req.xContentType === 'application/x-www-form-urlencoded') {
                    return res.status(500).json({key:key,error:new Error("BODY is empty using application/x-www-form-urlencoded, you may want to use application/json instead, especially for Array")}).end();
                }
                if (tuples.length === 1) {
                    key   = tuples[0].key;
                    value = tuples[0].value;
                }
            }
            res.header('X-redis-multi-key', false);
            res.header('X-redis-key', key);

            redis.set(key, value, function (err, result, serverURL) {
                if (err) {
                    return res.status(500).json({key:key,error:err}).end();
                }
                res.header('X-redis-server', serverURL);
                // io.sockets.emit('new:key', {key:req.params.key, value: req.body, server:serverURL});
                res.status(204).end();
            });
        });

        // DELETE
        app.delete('/redis/api/v1/key/:key?', function (req, res) {
            res.header('X-redis-http-method', 'DELETE');
            res.header('X-redis-operation', 'del');
            if (req.params.key) {
                res.header('X-redis-multi-key', false);
                res.header('X-redis-key', req.params.key);
                redis.del(req.params.key, function (err, value, serverURL) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,error:err}).end();
                    }
                    res.header('X-redis-server', serverURL);
                    // io.sockets.emit('delete:key', {key:req.params.key, server:serverURL});
                    return res.status(204).end();
                });
            } else {
                if (req.query.force === 'true') {
                    res.header('X-redis-multi-key', true);
                    redis.keys("*", function(err, keys, server) {
                        var total = 0;
                        var failed = 0;
                        var servers = {};
                        var errors = [];
                        function removeEntry(list) {
                            if (!list || list.length < 1) {
                                res.header('X-redis-server', Object.keys(servers).join(','));

                                if (failed === 0) {
                                    return res.status(204).end();
                                } else {
                                    return res.status(failed < total ? 206 : 500).json({
                                        keys: keys,
                                        errors: errors,
                                        total: total,
                                        failed: failed
                                    }).end();
                                }
                            }
                            var key = list.shift();
                            redis.del(key, function (err, res, serverURL) {
                                errors.push(err);
                                if (err) {
                                    failed += 1;
                                } else {
                                    servers[serverURL] = true;
                                    // io.sockets.emit('delete:key', {key:key, server:serverURL});
                                    total += 1;
                                }
                                process.nextTick(function () {
                                    removeEntry(list);
                                });
                            });
                        }

                        if (err) {
                            return res.status(500).json({key:'*',error:err}).end();
                        }
                        res.header('X-redis-key', keys.join(','));
                        servers[server.url] = true;
                        removeEntry(keys.slice(0));
                    });
                } else {
                    return res.status(400).json({error:new Error("To delete all the entries, you must use the 'force' option")}).end();
                }
            }
        });

        // GET status
        app.get('/redis/api/v1/status', function (req, res) {
            res.header('X-redis-http-method', 'GET');
            res.header('X-redis-operation', 'status');
            redis.status(function (err, value, server) {
                if (err) {
                    return res.status(500).json({error:err}).end();
                }
                return res.status(200).json(value).end();
            });
        });
    }

    return routes;
}();