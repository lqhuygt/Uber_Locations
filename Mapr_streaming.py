# # Need some packages to talk to Kafka.
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, concat_ws, concat
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel

# create spark
spark = SparkSession.builder.appName("Streaming").getOrCreate()

# read stream from kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "UberTopic").option("startingOffsets", "earliest").load()
df.printSchema()

# cast message value to string
df_cast = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df_uber = df_cast.select(col("value").cast("string")).alias("csv").select("csv.*")

# create df with message value
df_uber2 = df_uber.selectExpr("split(value,',')[0] as dt",
                               "split(value,',')[1] as lat",
                               "split(value,',')[2] as lon",
                               "split(value,',')[3] as base")
df_uber2.printSchema()

# convert column type
df_uber3 = df_uber2.withColumn("dt",to_timestamp("dt").cast("timestamp")) \
                    .withColumn("lat", col("lat").cast("double")) \
                    .withColumn("lon", col("lon").cast("double")) 
df_uber3.printSchema()

# Denfine features vector to use for kmeans algorithm
featureCols = ['lat', 'lon']
assembler = VectorAssembler(inputCols=featureCols, outputCol='features')
df_uber4 = assembler.transform(df_uber3)

# load model
model = KMeansModel.load("E:/PySpark/Uber_Locations/model/uber_location")

# make prediction
df_predicted = model.transform(df_uber4)

# add id column = cid + lat + lon
split_lon = split(df_predicted.lon, "\.").getItem(1)
split_lat = split(df_predicted.lat, "\.").getItem(1)
id = concat(split_lat,split_lon) # nối chuỗi
df_uber_id = df_predicted.withColumn("id", concat_ws("_",col("cid"),id)) # add column "id"

# drop feature column
df_uber_locates = df_uber_id.drop(df_uber_id.features)

# write stream to console 
streaming_uber = df_uber_locates.writeStream.format("console").outputMode("append").start().awaitTermination()

# wtite stream to hdfs
# warehouse = df_uber_locates.writeStream.format("csv") \
#                                         .option("path", "hdfs://localhost:9000/Uber_Warehouse/data_warehouse") \
#                                         .option("checkpointLocation", "hdfs://localhost:9000/Uber_Warehouse/checkpoints") \
#                                         .outputMode("append") \
#                                         .start()



