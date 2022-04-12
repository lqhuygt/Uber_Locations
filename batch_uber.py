from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Parsing the messeage value into dataframe
df_uber_batch_cast = df_uber_batch.select(col("value").cast("string")).alias("csv").select("csv.*")
df_uber_batch_casted = df_uber_batch_cast.selectExpr("split(value,',')[0] as dt",
                               "split(value,',')[1] as lat",
                               "split(value,',')[2] as lon",
                               "split(value,',')[3] as base")
df_uber_batch_casted.printSchema()

# df_uber2.writeStream.format("console").outputMode("append").start().awaitTermination()

path = "hdfs://localhost:9000/raws/"

# write to hdfs
push_to_hdfs = df_uber_batch_casted.write \
                    .mode("overwrite") \
                    .format("csv") \
                    .save(path)


