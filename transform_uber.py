
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, TimestampType
from pyspark.sql.functions import col, to_timestamp
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import split, concat_ws, concat


schema = StructType([ 
    StructField("dt",TimestampType(), True), 
    StructField("lat",DoubleType(), True), 
    StructField("lon",DoubleType(), True), 
    StructField("base", StringType(), True), 
  ])

spark = SparkSession.builder.appName('Uber')\
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.14')\
        .getOrCreate()

path = "hdfs://localhost:9000/raws/" 
df_uber = spark.read.csv(path=path, schema=schema)
df_uber.show(5)
df_uber.printSchema()


# Denfine features vector to use for kmeans algorithm
featureCols = ['lat', 'lon']
assembler = VectorAssembler(inputCols=featureCols, outputCol='features')

df_uber2 = assembler.transform(df_uber)
df_uber2.show(5)


# setK(20) phân thành 20 cụm
# setFeaturesCol("features") dùng để train
# setPredictionCol("cid") dùng để predict
kmeans = KMeans().setK(20).setFeaturesCol("features").setPredictionCol("cid").setSeed(1)
model = kmeans.fit(df_uber2)

# Shows the result 20 cluster.
centers = model.clusterCenters()
i=0
print("Cluster Centers: ")
for center in centers:
    print(i, center)
    i += 1

# make prediction
df_predicted = model.transform(df_uber2)
df_predicted.show(5)

# add id column = cid + lat + lon
split_lon = split(df_predicted.lon, "\.").getItem(1)
split_lat = split(df_predicted.lat, "\.").getItem(1)
id = concat(split_lat,split_lon) # nối chuỗi
df_uber_id = df_predicted.withColumn("id", concat_ws("_",col("cid"),id)) # add column "id"

# drop feature column
df_uber_locates = df_uber_id.drop(df_uber_id.features)
df_uber_locates.show(5)

#write to postgres
df_uber_locates.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/Test")\
    .option("dbtable", "TestUber") \
    .option("user", "postgres") \
    .option("password", "Huy12345678") \
    .option("driver", "org.postgresql.Driver") \
    .save()

spark.stop()




