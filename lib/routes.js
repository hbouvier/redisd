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
                        var object;
                        try {
                            object = JSON.parse(value);
                        } catch (e) {
                            return res.status(500).json({key:req.params.key,operation:"get",error:e,status:"FAILED",server:server.url}).end();
                        }
                        return res.json({key:req.params.key,value:object,operation:"get",status:"OK",server:server.url}).end();
                    }
                    return res.status(404).json({key:req.params.key,operation:"get",status:"NOT-FOUND"}).end();
                });
            } else {
                redis.mgetkeys("*", function (err, values, servers) {
                    if (err) {
                        return res.status(500).json({key:req.params.key,operation:"mgetkeys",error:err,status:"FAILED"}).end();
                    }
                    if (values) {
                        var objects = values.map(function (tuple) {
                            try {
                                return {key: tuple.key, value: JSON.parse(tuple.value)};
                            } catch (e) {
                                return {key: tuple.key, exception:e};
                            }
                        });
                        return res.json({objects:objects,operation:"mgetkeys",status:"OK", servers:[servers[0].url, servers[1].url]}).end();
                    }
                    return res.status(404).json({key:req.params.key,operation:"mgetkeys",status:"NOT-FOUND"}).end();
                });
            }
        });

        // POST
        app.post('/redis/api/v1/key', function (req, res) {
            var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });

            redis.set(uuid, JSON.stringify(req.body), function (err, result, server) {
                if (err) {
                    return res.status(500).json({key:uuid,operation:"set",error:err,status:"FAILED"}).end();
                }
                io.sockets.emit('new:key', {key:uuid, value: req.body, server:server.url});
                res.status(200).json({key:uuid,value:req.body,operation:"set",status:'OK', server:server.url}).end();
            });
        });

        // PUT
        app.put('/redis/api/v1/key/:key', function (req, res) {
            redis.set(req.params.key, JSON.stringify(req.body), function (err, result, server) {
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