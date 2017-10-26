#!/bin/bash

# Create kakfa topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic event_data_topic --partitions 4 --replication-factor 2

# describe the topics and see who takes care of which partition
# /usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181

# Spawn kafka producer threads as shown below from publisher location 
# bash spawn_producer_threads.sh $ANY_CLUSTER_IP 8 1


