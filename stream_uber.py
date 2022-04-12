from pyspark.sql import SparkSession
import time

# create spark
spark = SparkSession.builder.appName('Streaming')\
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()

# read stream from kafka
# earliest
df_uber = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "StreamUber") \
      .option("startingOffsets", "latest") \
      .load()
df_uber.printSchema()

format_datetime = time.strftime('%Y-%m-%d_%H-%M-%S')
checkpoint = "hdfs://localhost:9000/checkpoints_kafka/checkpoint-{}/".format(format_datetime)

# write to batch topic
batch_topic_writer = df_uber.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "BatchUber") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint) \
    .start()

batch_topic_writer.awaitTermination()





