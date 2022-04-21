from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField


# create spark
spark = SparkSession.builder.appName('Batch')\
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()

# read stream from kafka
df_uber_batch = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "BatchUber") \
      .option("startingOffsets", "earliest") \
      .load()
df_uber_batch.printSchema()

# create schema
schema = StructType([ 
    StructField("dt",StringType(), True), 
    StructField("lat",StringType(), True), 
    StructField("lon",StringType(), True), 
    StructField("base", StringType(), True), 
  ])

# Parsing the messeage value into dataframe
df_uber_batch_cast = df_uber_batch.select(from_json(col("value").cast("string"), schema).alias("value"))
df_uber_batch_casted = df_uber_batch_cast.selectExpr("value.dt", "value.lat", "value.lon", "value.base")
df_uber_batch_casted.printSchema()
df_uber_batch_casted.show(5)

# df_uber2.writeStream.format("console").outputMode("append").start().awaitTermination()

path = "hdfs://localhost:9000/raws/"

# write to hdfs
push_to_hdfs = df_uber_batch_casted.write \
                    .mode("overwrite") \
                    .format("csv") \
                    .save(path)


