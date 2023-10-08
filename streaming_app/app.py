from pyspark import *
from pyspark.sql import *
from pyspark.sql.utils import *
from pyspark.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import *



output_path_bikes = "hdfs://hdfs-namenode:9000/data/bikes_result"
output_path_bikeshops = "hdfs://hdfs-namenode:9000/data/bikeshops_result"
output_path_orders = "hdfs://hdfs-namenode:9000/data/orders_result"


spark = SparkSession \
    .builder \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "bikes,bikeshops,orders") \
  .load()


table = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)" , "topic")


# Modify the "path" option in the writeStream operation for each topic
query_bikes = table.filter(col("topic") == "bikes").writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "checkpoint_bikes") \
    .option("path", output_path_bikes) \
    .start()

query_bikeshops = table.filter(col("topic") == "bikeshops").writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "checkpoint_bikeshops") \
    .option("path", output_path_bikeshops) \
    .start()

query_orders = table.filter(col("topic") == "orders").writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "checkpoint_orders") \
    .option("path", output_path_orders) \
    .start()

# Await termination for each query


# query = table.writeStream.outputMode("append").format("console").start()

f = open("healthy", "w")
f.write("healthy")
f.close()

query_bikes.awaitTermination()
query_bikeshops.awaitTermination()
query_orders.awaitTermination()