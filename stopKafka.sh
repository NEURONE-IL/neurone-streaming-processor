#!/bin/bash


# curl -X PUT http://localhost:8084/connectors/mongo-source/pause
$KAFKA_HOME/bin/kafka-streams-application-reset.sh --application-id streaming-processing --input-topics test.visitedlinks test.bookmarks totalcover bmrelevant precision --force
$KAFKA_HOME/bin/kafka-server-stop.sh
$KAFKA_HOME/bin/zookeeper-server-stop.sh
