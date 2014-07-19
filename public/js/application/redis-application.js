// To have one place where we define both the 'url' use in the javascript pages and the routes (because the
// $routeProvider cannot use the $rootScope, we have to define a global (beuark) variable here.
var ___g_redisRoutePrefix___ = '/';
angular.module('redis', ['ngRoute', 'redisServices', 'redisControllers'])
    .run(function ($rootScope) {
        $rootScope.baseAPIurl = '/redis/api/v1';
        $rootScope.baseUIurl = '/';
        $rootScope.urlBasePath = ___g_redisRoutePrefix___;
    })
    .config(['$routeProvider', '$locationProvider', function ($routeProvider, $locationProvider) {
        $routeProvider.
            when(___g_redisRoutePrefix___, {controller: 'redisCtrl', templateUrl: 'views/redis.html'}).
            otherwise({redirectTo: ___g_redisRoutePrefix___});
    }])
;
