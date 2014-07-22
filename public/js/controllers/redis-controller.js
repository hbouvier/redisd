angular.module('redisControllers', ['redisServices', 'redisModels'])
    .controller('redisCtrl', function($scope, $location, socket, KeyStore, KeyStoreStats) {
    	$scope.redis = {
    		keyStore  : KeyStore.list(function(data, headers) {
                $scope.redis.headers = {};
                $scope.redis.headers.operation  = headers('X-redis-operation');
                $scope.redis.headers.multikey   = headers('X-redis-multi-key');
                $scope.redis.headers.key        = headers('X-redis-key');
                $scope.redis.headers.server     = headers('X-redis-server');
                $scope.redis.headers.httpmethod = headers('X-redis-http-method');
            }),
        	stats     : KeyStoreStats.get()
        };
        socket.on('redis:set', function (message) {
        	var found  = false,
                object = JSON.parse(message);
        	if ($scope.redis.keyStore) {
                for (var name in $scope.redis.keyStore) {
                    if ($scope.redis.keyStore.hasOwnProperty(name) && name === object.key) {
	        			found = true;
	        			$scope.redis.keyStore[object.key] = object.value;
	        			break;
	        		}
	        	}
	        }
        	if (!found) {
        		if (!$scope.redis.keyStore)
        			$scope.redis.keyStore = {};
        		$scope.redis.keyStore[object.key] = object.value;
        	}
        });
        socket.on('redis:del', function (message) {
            var object = JSON.parse(message);
            if ($scope.redis.keyStore && $scope.redis.keyStore.hasOwnProperty(object.key)) {
                delete $scope.redis.keyStore[object.key];
            }
        });

    })
;
