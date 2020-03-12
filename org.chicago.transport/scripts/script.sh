if [ -z $1 ]; then

	kafka-topics --zookeeper localhost:2181 --list
else
	kafka-console-consumer --bootstrap-server localhost:9092 --topic $1 --from-beginning
fi
