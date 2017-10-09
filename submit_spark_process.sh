#!/bin/bash

#PYSCRP=$1
#SPARK_IP_ADDR=$2
#KAFKA_MASTER=$3
#spark-submit --master "spark://$SPARK_IP_ADDR:7077" --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2  $PYSCRP $KAFKA_MASTER

PYSCRP=$1
SPARK_IP_ADDR=$2
KAFKA_MASTER=$3
spark-submit --master "spark://$SPARK_IP_ADDR:7077" --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2   $PYSCRP $KAFKA_MASTER

#PY_SCRP=$1
#SPARK_MASTER=$2
#KAFKA_MASTER=$3
#CASSANDRA_WORKERS=$4
#spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 $PY_SCRP $KAFKA_MASTER

#PY_SCRP=$1
#SPARK_MASTER=$2
#KAFKA_MASTER=$2
#POSTGRES_IP=$3
#spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$POSTGRES_IP --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 $PY_SCRP $KAFKA_MASTER



