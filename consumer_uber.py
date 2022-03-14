
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# create spark
spark = SparkSession.builder.appName('Streaming')\
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()

# read stream from kafka
df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "DemoTopic") \
      .option("startingOffsets", "earliest") \
      .load()
df.printSchema()

# Parsing the messeage value into dataframe
df_uber = df.select(col("value").cast("string")).alias("csv").select("csv.*")
df_uber2 = df_uber.selectExpr("split(value,',')[0] as dt",
                               "split(value,',')[1] as lat",
                               "split(value,',')[2] as lon",
                               "split(value,',')[3] as base")
df_uber2.printSchema()

# df_uber2.writeStream.format("console").outputMode("append").start().awaitTermination()
fomart_datetime = time.strftime('%Y-%m-%d_%H-%M-%S')
path = "hdfs://localhost:9000/raws/raw_{}".format(fomart_datetime)
checkpoint = "hdfs://localhost:9000/checkpoints/checkpoint-{}/".format(fomart_datetime)

push_to_hdfs = df_uber2.writeStream.format("csv") \
                    .option("path", path) \
                    .option("checkpointLocation", checkpoint) \
                    .outputMode("append") 

push_to_hdfs.start().awaitTermination()



