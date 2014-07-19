[![Build Status](https://travis-ci.org/hbouvier/redisd.png)](https://travis-ci.org/hbouvier/redisd)
[![dependency Status](https://david-dm.org/hbouvier/redisd/status.png?theme=shields.io)](https://david-dm.org/hbouvier/redisd#info=dependencies)
[![devDependency Status](https://david-dm.org/hbouvier/redisd/dev-status.png?theme=shields.io)](https://david-dm.org/hbouvier/redisd#info=devDependencies)
[![NPM version](https://badge.fury.io/js/redisd.png)](http://badge.fury.io/js/redisd)

Redisd
===

A Redis REST API with basic WebUI

## Installation

	brew install redis
	sudo npm install -g redisd

## Startup

	/usr/local/opt/redis/bin/redis-server /usr/local/etc/redis.conf >& /tmp/redis.log &
	nohup redisd >& /tmp/redisd.log &

## Web UI

	open http://localhost:8080

## REDIS CONFIGURATION

	REDIS_URLS  (default: 127.0.0.1:6379)

## REST ROUTES

	* GET /redis/api/v1/key

		List all keys in redis

	* GET /redis/api/v1/key/{key}

		Return the value associated with that key

	* POST /redis/api/v1/key

		Generate a UUID to use as KEY and associate the value in redis.
		
	* PUT /redis/api/v1/key/{key}

		Create or Modify the value of a key

	* DELETE /redis/api/v1/key/{key}

		Remove the key from redis

	* DELETE /redis/api/v1/key?force=true

		Remove all keys from redis

	* GET /redis/api/v1/status

		Return some basic stats

