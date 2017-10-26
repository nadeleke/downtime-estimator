#!/bin/bash

PYTHON_SCRP=$1
SPARK_MASTER_IP=$2
KAFKA_MASTER_IP=$3
REDIS_MASTER_IP=$4
spark-submit --master "spark://$SPARK_MASTER_IP:7077" --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,org.postgresql:postgresql:42.1.1,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 --driver-class-path ~/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar $PYTHON_SCRP $KAFKA_MASTER_IP $REDIS_MASTER_IP


