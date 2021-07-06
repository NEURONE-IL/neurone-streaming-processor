#!/bin/bash


$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 2
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 2
# $KAFKA_HOME/bin/connect-standalone.sh -daemon $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/MongoSourceConnector.properties 
# sleep 2

$KAFKA_HOME/bin/kafka-topics.sh --create --topic test.visitedlinks --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic test.bookmarks --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic connect-config --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic connect-offset --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic connect-status --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

#


# docker-compose exec mongo /usr/bin/mongo -u root -p admin --eval '''if (rs.status()["ok"] == 0) {
#     rsconf = {
#       _id : "rs0",
#       members: [
#         { _id : 0, host : "mongo:27017", priority: 1.0 },
#       ]
#     };
#     rs.initiate(rsconf);
# }

# rs.conf();'''

