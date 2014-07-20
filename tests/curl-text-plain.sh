#!/bin/sh
cat input.csv | while read line ; do 
	key=`echo $line | cut -f1 -d,`
	value=`echo $line | cut -f2-99 -d,`
	echo "---------- $key === $value --------"
	curl -H 'Content-Type: text/plain' -X PUT http://127.0.0.1:8080/redis/api/v1/key/$key -d "$value"
	printf "\n"
        if [ "$key" == "object" -o "$key" == "vector" ] ; then
		curl -s http://127.0.0.1:8080/redis/api/v1/key/$key | json value | json
		printf "\n"
                redis-cli get $key | json
        else
		curl -s http://127.0.0.1:8080/redis/api/v1/key/$key | json value 
		printf "\n"
                redis-cli get $key
        fi
done
