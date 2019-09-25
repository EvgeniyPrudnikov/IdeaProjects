#start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
#start kakfa
bin/kafka-server-start.sh config/server.properties

# put sample data to topic
cat /home/jk/IdeaProjects/hw/src/main/scala/resources/kafka-messages.jsonline | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic homework

#check topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092


# bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test


# read from homework topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic homework \
--from-beginning

#read from result topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic homework_result \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
