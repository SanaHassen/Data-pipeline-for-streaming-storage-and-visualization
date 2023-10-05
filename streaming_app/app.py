from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingToHDFS") \
    .getOrCreate()

# Define schema for orders, bikes, and bikeshops
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_line", IntegerType()),
    StructField("order_date", DateType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType())
])

bike_schema = StructType([
    StructField("bike_id", IntegerType()),
    StructField("model", StringType()),
    StructField("category1", StringType()),
    StructField("category2", StringType()),
    StructField("frame", StringType()),
    StructField("price", DecimalType())
])

bikeshop_schema = StructType([
    StructField("bikeshop_id", IntegerType()),
    StructField("bikeshop_name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("latitude", DecimalType()),
    StructField("longitude", DecimalType())
])

# Create a streaming context
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)  # 5-second batches





# Subscribe to 1 topic, with headers
orders_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "orders") \
  .load()
orders_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

bikes_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "bikes") \
  .load()
bikes_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

bikeshops_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "bikeshops") \
  .load()
bikeshops_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


print(orders_df)

# Perform required transformations and processing here
# For example, you can join the DataFrames, aggregate data, etc.

# Save the results to HDFS
# orders_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/orders")
# bikes_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/bikes")
# bikeshops_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/bikeshops")


# Start the streaming context
ssc.start()
ssc.awaitTermination()
