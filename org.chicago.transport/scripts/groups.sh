if [ -z $1 ]; then
	kafka-consumer-groups --bootstrap-server localhost:9092 --list
else
	kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $1
fi
