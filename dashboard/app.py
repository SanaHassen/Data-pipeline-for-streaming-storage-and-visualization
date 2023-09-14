from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
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
sc = SparkContext(appName="KafkaStreamingToHDFS")
ssc = StreamingContext(sc, 5)  # 5-second batches

# Define Kafka topics to consume from
kafka_params = {"bootstrap.servers": "localhost:9092"}
topics = ["orders", "bikes", "bikeshops"]

# Create a Kafka stream
kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

# Process the incoming Kafka stream
def process_stream(rdd):
    if not rdd.isEmpty():
        # Convert JSON messages to DataFrames
        orders_df = spark.read.schema(order_schema).json(rdd.map(lambda x: x[1]))
        bikes_df = spark.read.schema(bike_schema).json(rdd.map(lambda x: x[1]))
        bikeshops_df = spark.read.schema(bikeshop_schema).json(rdd.map(lambda x: x[1]))

        # Perform required transformations and processing here
        # For example, you can join the DataFrames, aggregate data, etc.

        # Save the results to HDFS
        orders_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/orders")
        bikes_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/bikes")
        bikeshops_df.write.mode("append").parquet("hdfs://hdfs-namenode:9000/bikeshops")

# Process the Kafka stream
kafka_stream.foreachRDD(process_stream)

# Start the streaming context
ssc.start()
ssc.awaitTermination()
