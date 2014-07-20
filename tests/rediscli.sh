#!/bin/sh
cat input.csv | while read line ; do 
	key=`echo $line | cut -f1 -d,`
	value=`echo $line | cut -f2-99 -d,`
	echo "---------- $key === $value --------"
	redis-cli set $key "$value"
	if [ "$key" == "object" -o "$key" == "vector" ] ; then
		redis-cli get $key | json
	else
		redis-cli get $key
	fi
done
