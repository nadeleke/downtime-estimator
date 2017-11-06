# Downtime Estimator

Downtime means different things in different industries. But fundamentally, downtime can be described as the duration of time within which revenue generating operations/processes are put on hold typically for inevitable reasons. In anycase, the ability to estimate downtime aids the minimization of financial losses due to downtime.

The business case that inspired the development of this tool is downtime estimation during oil production. Wells that use artificial-lift pumps such as sucker-rod pumps need to be shutdown when fluid levels drops below a certain allowable threshold within the wellbore. As fluid levels rise due to fluid influx from the reservoir, beam pumps are used to produce the oil in the wellbore, which in turn results in a drop in fluid level over time. Failure to shut down the pumps as fluid levels drop below the allowable threshold would damage the pump. Pump remediation operations are costly and must avoided.

Sensor data from pumps and pattern recognition are used to determine when a pump should be shutdown. The same pump data is no longer available once the pump has been shutdown. However, pump metadata indicating when the pump was shutdown, started-up and how much was produced in the time period the pump was running remains available. While the pump is off, valuable oil production is halted. The ability to estimate the amount of time it will take for the fluid level in the wellbore to rise above an acceptable threshold above the pumps can be turned on and oil production can commence is crucial.

Our goal was to develop a data-driven methodology for estimating production downtown. The following is the outline explains the components of the downtime estimator.

1. [Data Generation](README.md#data-generation)
2. [Data Ingestion using Kafka](README.md#data-ingestion-using-kafka)
3. [Distributed Computing using Spark](README.md#distributed-computing-using-spark)
4. [Data Storage using Redis Cache](README.md#data-storage-using-redis-cache)
5. [The Pipeline](README.md#the-pipeline)
6. [Demo](README.md#demo)
7. [Setup Steps](README.md#setup-steps)
8. [FAQ](README.md#faq)

## Data Generation
Data was generated to simulate pump metadata. The data includes well ID, event timestamp, event type (shutdown or startup), production volume at shutdown, well completion info., wellbore volume, field, well lat-lng, state. Typically, this information would be collected on scada systems that house operation data. 

## Data Ingestion using Kafka
Data ingestion for the data stream was published on a distributed kafka cluster on AWS ec2 (an elastic cloud computing platform). The `src/event-producer.py` script generates the pump metadata and sends them to the kafka queue.

## Distributed Computing using Spark
Data consumption and distributed computing is conducted using spark-streaming's streaming session, which is a micro-batching module in the pyspark library. Computations are distributed on four ec2 nodes (1 master and 3 workers). Zookeeper and spark were installed on these nodes using pegasus (a VM based deployment tool for prototyping other Big Data tools on Amazon Web Services). The `src/event-consumer.py` script is the pyspark code that consumes the data from the kafka queue and processes them real-time in a distributed manner. This code also saves the original data to AWS S3, which is a highly scalable data repository.The same code also saved processed data result and recent history well parameters into redis. 

## Data Storage using Redis Cache
The result of the computation along with recent history data for individual wells is stored in a replicated redis node. The reason for this is to prevent data loss in case of node failures in addition to the high availabilty advantage of redis cache. 

## The Pipeline
| ![Figure 1](./images/Streaming_Pipeline.png) | 
|:--:| 
| *Figure 1: A real-time streaming and processing data pipeline for downtime estimation* |

## Demo
[![Watch the video](./images/youtube-vid.jpg)](https://youtu.be/QoT7N33Ha1M)

## Setup Steps
Coming soon!

## FAQ
Coming soon!

Send questions to nadeleke@gmail.com

