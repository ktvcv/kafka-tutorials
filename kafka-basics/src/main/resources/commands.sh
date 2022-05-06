 kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3 --replication-factor 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo_java

# generate a Kafka UUID
kafka-storage.sh random-uuid

# generate a Kafka UUID
# This returns a UUID, for example 76BLQI7sT_ql1mBfKsOk9Q
kafka-storage.sh format -t nWV09h-9T5KBIgqpTAzeTg -c ~/kafka_2.13-3.1.0/config/kraft/server.properties

# This will format the directory that is in the log.dirs in the config/kraft/server.properties file

# start Kafka
kafka-server-start.sh ~/kafka_2.13-3.1.0/config/kraft/server.properties