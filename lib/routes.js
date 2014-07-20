module.exports = function () {

    function routes(app, config, io) {
        var redis  = config.redis;

        // GET
        app.get('/redis/api/v1/key/:key?', function (req, res) {
            if (req.params.key) {
                redis.get(req.params.key, function (err, value, server) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,operation:"get",error:err,status:"FAILED"}).end();
                    }
                    if (value) {
                        return res.json({key:req.params.key,value:value,operation:"get",status:"OK",server:server.url}).end();
                    }
                    return res.status(404).json({key:req.params.key,operation:"get",status:"NOT-FOUND"}).end();
                });
            } else {
                redis.mgetkeys("*", function (err, values, servers) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,operation:"mgetkeys",error:err,status:"FAILED"}).end();
                    }
                    if (values) {
                        return res.json({values:values,operation:"mgetkeys",status:"OK", servers:[servers[0].url, servers[1].url]}).end();
                    }
                    return res.status(404).json({pattern:'*',operation:"mgetkeys",status:"NOT-FOUND"}).end();
                });
            }
        });

        // POST
        app.post('/redis/api/v1/key', function (req, res) {
            var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });

            var tuples = [];
            if (req.xContentType === 'application/x-www-form-urlencoded') {
                for (var name in req.body) {
                    if (req.body.hasOwnProperty(name)) {
                        tuples.push({key: name, value: req.body[name]});
                    }
                }
                // Invalid Post using something like curl
                if (tuples.length === 1 && !tuples[0].value) {
                    tuples[0].value = tuples[0].key;
                    tuples[0].key = uuid;
                }
                var total  = 0,
                    failed = 0;
                function _set(list, server_url) {
                    if (!list || list.length < 1) {
                        console.log('tuples:', tuples)
                        if (tuples.length === 1) {
                            return res.status(failed === 0 ? 200 : 500).json({
                                key: tuples[0].key,
                                value: tuples[0].value,
                                operation: "set",
                                status: failed === 0 ? "OK" : "FAILED",
                                server: server_url}).end();
                        } else {
                            return res.status(failed < total ? 200 : 500).json({
                                keys: tuples.map(function (obj) {
                                    return obj.key;
                                }),
                                value: tuples.map(function (obj) {
                                    return obj.value;
                                }),
                                total: total, failed: failed,
                                status: failed < total ? "OK" : "FAILED",
                                operation: "set", server: server_url}).end();
                        }
                    }
                    var obj = list.shift();
                    redis.set(obj.key, obj.value, function (err, result, server) {
                        if (err) {
                            failed += 1;
                        } else {
                            total += 1;
                            io.sockets.emit('new:key', {key: obj.key, value: obj.value, server: server.url});
                        }
                        process.nextTick(function () {
                            _set(list, server.url);
                        });
                    });
                }
                console.log('tuples:', tuples)
                _set(tuples.slice(0));
            } else {
                redis.set(key, req.body, function (err, result, server) {
                    if (err) {
                        return res.status(500).json({key:key,operation:"set",error:err,status:"FAILED"}).end();
                    }
                    io.sockets.emit('new:key', {key:key, value: req.body, server:server.url});
                    res.status(200).json({key:key,value:req.body,operation:"set",status:'OK', server:server.url}).end();
                });
            }
        });

        // PUT
        app.put('/redis/api/v1/key/:key', function (req, res) {
            if (req.xContentType === 'application/x-www-form-urlencoded') {
                if (!req.body) {
                    return res.status(500).json({key:req.params.key,operation:"set",error:new Error("BODY is empty using application/x-www-form-urlencoded, you may want to use application/json instead, especially for Array"),status:"FAILED"}).end();
                }
                var tuples = [];
                for (var name in req.body) {
                    if (req.body.hasOwnProperty(name)) {
                        tuples.push({key: name, value: req.body[name]});
                    }
                }
                // Invalid Post using something like curl
                if (tuples.length === 1 && !tuples[0].value) {
                    config.logger.log('warn', 'Content-Type: %s|value-was=%j|fixed-value=%j', req.xContentType, req.body, tuples[0].key, config.meta);
                    req.body = tuples[0].key;
                }
            }
            redis.set(req.params.key, req.body, function (err, result, server) {
                if (err) {
                    return res.status(500).json({key:req.params.key,operation:"set",error:err,status:"FAILED"}).end();
                }
                io.sockets.emit('new:key', {key:req.params.key, value: req.body, server:server.url});
                res.status(200).json({key:req.params.key,value:req.body,operation:"set",status:'OK', server:server.url}).end();
            });
        });

        // DELETE
        app.delete('/redis/api/v1/key/:key?', function (req, res) {
            if (req.params.key) {
                redis.del(req.params.key, function (err, value, server) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,operation:"delete",error:err,status:"FAILED"}).end();
                    }
                    io.sockets.emit('delete:key', {key:req.params.key, server:server.url});
                    return res.json({key:req.params.key,operation:"delete",status:"OK", server:server.url}).end();
                });
            } else {
                if (req.query.force === 'true') {
                    var total = 0;
                    var failed = 0;
                    var servermap = {};
                    function removeEntry(keys) {
                        if (!keys || keys.length < 1) {
                            return res.json({key:"*",force:true, operation:"delete",total:total,failed:failed,status:"OK", servers:servermap}).end();
                        }
                        var key = keys.shift();
                        redis.del(key, function (err, res, server) {
                            if (err) {
                                failed += 1;
                            } else {
                                if (!servermap[server.url])
                                    servermap[server.url] = 0;
                                servermap[server.url] += 1 ;
                                io.sockets.emit('delete:key', {key:key, server:server.url});
                                total += 1;
                            }
                            process.nextTick(function () {
                                removeEntry(keys);
                            });
                        });
                    }
                    redis.keys("*", function(err, keys, server) {
                        if (err) {
                            return res.status(500).json({key:"*",force:true,operation:"delete",error:err,status:"FAILED"}).end();
                        }
                        servermap[server.url] = 1;
                        removeEntry(keys)
                    });
                } else {
                    return res.status(400).json({key:"*",operation:"delete",error:new Error("To delete all the entries, you must use the 'force' option"),status:"FAILED"}).end();
                }
            }
        });

        // GET status
        app.get('/redis/api/v1/status', function (req, res) {
            redis.status(function (err, value) {
                if (err) {
                    return res.status(500).json({operation:"status",error:err,status:"FAILED"}).end();
                }
                return res.json({status:'OK',operation:"status", status:"OK", stats:value}).end();
            });
        });
    }

    return routes;
}();