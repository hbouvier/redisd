angular.module('redisServices', ['ngResource'])
    .filter('objectFilter', function ($rootScope) {
        return function (input, query) {
            if (!query) return input;
            var result = [];

            angular.forEach(input, function (object) {
                var copy = {};
                var regex = new RegExp(query, 'im');
                for (var i in object) {
                    // angular adds '$$hashKey' to the object.
                    if (object.hasOwnProperty(i) && i !== '$$hashKey')
                        copy[i] = object[i];
                }
                if (JSON.stringify(copy).match(regex)) {
                    result.unshift(object);
                }
            });
            return result;
        };
    })
    .factory('socket', function($rootScope) {
        var WebDomain  = document.domain,
            socket = io.connect("");
            return {
                on: function(eventName, callback) {
                    socket.on(eventName, function() {
                        var args = arguments;
                        $rootScope.$apply(function () {
                            callback.apply(socket, args);
                        });
                    });
                },
                emit: function (eventName, data, callback) {
                    socket.emit(eventName, data, function() {
                        var args = arguments;
                        $rootScope.$apply(function() {
                            if (callback)
                                callback.apply(socket, args);
                        });
                    });
                }
            };
    })

    .factory('KeyStore', function ($rootScope, $resource) {
        return $resource($rootScope.baseAPIurl + '/key/:key?', null, {
            "list"    : { method : "GET", isArray : false  },
            "get"     : { method : "GET", isArray : false },
            "put"     : { method : "PUT" },
            "post"    : { method : "POST" },
            "delete"  : { method : "DELETE" }
        });
    })
    .factory('KeyStoreStats', function ($rootScope, $resource) {
        return $resource($rootScope.baseAPIurl + '/status', null, {
            "get"     : { method : "GET", isArray : true }
        });
    })
;


