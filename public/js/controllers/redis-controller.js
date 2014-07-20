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
        socket.on('new:key', function (object) {
        	var found = false;
        	if ($scope.redis.keyStore.objects) {
	        	for (var i = 0 ; i < $scope.redis.keyStore.objects.length ; ++i) {
	        		if ($scope.redis.keyStore.objects[i].key === object.key) {
	        			found = true;
	        			$scope.redis.keyStore.objects[i].value = object.value;
	        			break;
	        		}
	        	}
	        }
        	if (!found) {
        		if (!$scope.redis.keyStore.objects)
        			$scope.redis.keyStore.objects = [];
        		$scope.redis.keyStore.objects.push(object);
        	}
        });
        socket.on('delete:key', function (object) {
        	if ($scope.redis.keyStore.objects) {
	        	for (var i = 0 ; i < $scope.redis.keyStore.objects.length ; ++i) {
	        		if ($scope.redis.keyStore.objects[i].key === object.key) {
	        			$scope.redis.keyStore.objects.splice(i, 1);
	        			break;
	        		}
	        	}
	        }
        });

    })
;
