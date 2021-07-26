# Build data pipeline analysis of popular Uber locations
### 1. Cluster uber
**Using SparkML to train model**
- read data csv file 
- create "features" column to train and clustering
- save model trained
### 2. Streaming uber
**Using Spark structure streaming + Apache Kafka to buid pipeline**
- add some packages to talk to Kafka.
  - spark-streaming-kafka-0-10_2.12:3.1.1
  - spark-sql-kafka-0-10_2.12:3.1.1
- push data on kafka
- read stream data from kafka consumer
- load model and predict data
- transfomation data
- write stream data to HDFS
### 3. Analyst uber
**Using Spark SQL + Matplotlib to analyze and visualize data**
- Which clusters had the highest number of pickups?
- Which hours of the day had the highest number of pickups?
- Which clusters had the highest number of pickups during morning rush hour?
- Which clusters had the highest number of pickups during afternoon rush hour?
- Which clusters had the highest number of pickups during evening rush hour?
