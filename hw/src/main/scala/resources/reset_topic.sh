#! /usr/bin/bash
#bash /home/jk/kafka_2.12-2.3.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic homework
#bash /home/jk/kafka_2.12-2.3.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic homework_result
cat /home/jk/IdeaProjects/hw/src/main/scala/resources/kafka-messages.jsonline | /home/jk/kafka_2.12-2.3.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic homework