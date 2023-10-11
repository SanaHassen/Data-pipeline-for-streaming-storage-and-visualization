from pyspark import *
from pyspark.sql import *
from pyspark.sql.utils import *
from pyspark.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import *



output_path_bikes = "cassandra://hdfs-namenode:9000/data/bikes_result"
output_path_bikeshops = "hdfs://hdfs-namenode:9000/data/bikeshops_result"
output_path_orders = "hdfs://hdfs-namenode:9000/data/orders_result"


 

spark = SparkSession.builder \
    .appName("KafkaToCassandraETL") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "bikes,bikeshops,orders") \
  .load()


table = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)" , "topic")

"""
table = df.selectExpr("CAST(value AS STRING)" , "topic")

split_cols = split(df['value'], ';')
df = df.withColumn('value1', split_cols[0].cast('string'))
df = df.withColumn('value2', split_cols[1].cast('string'))
df = df.withColumn('value3', split_cols[2].cast('string'))
df = df.withColumn('value4', split_cols[3].cast('string'))

# Select and rename the columns to match the Cassandra table
df_bikes = df.filter(col("topic") == "bikes").select(
    col('value1').alias('value1_c'),
    col('value2').alias('value2_c'),
    col('value3').alias('value3_c'),
    col('value4').alias('value4_c'),
    col('topic')
)

query_bikes = df_bikes.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_bikes") \
    .option("table", "bikes") \
    .option("keyspace", "default") \
    .start()

"""
# Modify the "path" option in the writeStream operation for each topic
query_bikes = table.filter(col("topic") == "bikes").writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_bikes") \
    .option("table", "bikes") \
    .option("keyspace", "default") \
    .start()

"""query_bikeshops = table.filter(col("topic") == "bikeshops").writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_bikeshops") \
    .option("table", "bikeshops") \
    .option("keyspace", "default") \
    .start()

query_orders = table.filter(col("topic") == "orders").writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_orders") \
    .option("table", "orders") \
    .option("keyspace", "default") \
    .start()"""

# Await termination for each query


# query = table.writeStream.outputMode("append").format("console").start()

f = open("healthy", "w")
f.write("healthy")
f.close()

query_bikes.awaitTermination()
query_bikeshops.awaitTermination()
query_orders.awaitTermination()