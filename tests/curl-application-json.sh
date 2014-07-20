#!/bin/sh
curl -vsw "\nHTTP-CODE=%{http_code}" -X DELETE 'http://127.0.0.1:8080/redis/api/v1/key?force=true'
cat input.csv | while read line ; do
	key=`echo $line | cut -f1 -d,`
	value=`echo $line | cut -f2-99 -d,`
	echo "---------- $key === $value --------"
	curl -vsw "\nHTTP-CODE=%{http_code}" -H 'Content-Type: application/json' -X PUT http://127.0.0.1:8080/redis/api/v1/key/$key -d "$value" 2>&1 | grep -v -E '^(\*|>|< HTTP|< X-Pow|< Content|< ETag|< Date|< Connection|\{)' | grep -v 'data not shown'
	printf "\n"
        if [ "$key" == "vector" -o "$key" == "object" ] ; then
		curl -vsw "\nHTTP-CODE=%{http_code}" http://127.0.0.1:8080/redis/api/v1/key/$key 2>&1 | grep -v -E '^(\*|>|< HTTP|< X-Pow|< Content|< ETag|< Date|< Connection)' | grep -v 'data not shown' > /tmp/$$.log
		echo "HEADERS:"
		cat /tmp/$$.log | grep -E '^(<)'
		echo "JSON:"
		cat /tmp/$$.log | grep -v -E '^(<)'
		rm /tmp/$$.log
		printf "\nREDIS: "
                redis-cli get $key
    else
		curl -vsw "\nHTTP-CODE=%{http_code}" http://127.0.0.1:8080/redis/api/v1/key/$key 2>&1 | grep -v -E '^(\*|>|< HTTP|< X-Pow|< Content|< ETag|< Date|< Connection|\{)' | grep -v 'data not shown'
		printf "\nREDIS: "
                redis-cli get $key
    fi
done
