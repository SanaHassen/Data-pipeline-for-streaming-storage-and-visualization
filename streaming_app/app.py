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


# table = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)" , "topic")


table = df.selectExpr("CAST(value AS STRING)" , "topic")


df_bikes = table.filter(table['topic'] == 'bikes')
df_bikeshops = table.filter(table['topic'] == 'bikeshops')
df_orders = table.filter(table['topic'] == 'orders')


df_bikes = df_bikes.withColumn('id', split(df_bikes['value'], ';')[0])
df_bikes = df_bikes.withColumn('model', split(df_bikes['value'], ';')[1])
df_bikes = df_bikes.withColumn('category1', split(df_bikes['value'], ';')[2])
df_bikes = df_bikes.withColumn('category2', split(df_bikes['value'], ';')[3])
df_bikes = df_bikes.withColumn('frame', split(df_bikes['value'], ';')[4])
df_bikes = df_bikes.withColumn('price', split(df_bikes['value'], ';')[5].cast('DECIMAL(10,2)'))


# Drop the original 'value' column
df_bikes = df_bikes.drop('value')
df_bikes = df_bikes.drop('topic')







# Assuming df_bikeshops is your DataFrame containing the 'value' column
df_bikeshops = df_bikeshops.withColumn('id', split(df_bikeshops['value'], ';')[0])
df_bikeshops = df_bikeshops.withColumn('name', split(df_bikeshops['value'], ';')[1])
df_bikeshops = df_bikeshops.withColumn('city', split(df_bikeshops['value'], ';')[2])
df_bikeshops = df_bikeshops.withColumn('state', split(df_bikeshops['value'], ';')[3])
df_bikeshops = df_bikeshops.withColumn('latitude', regexp_replace(split(df_bikeshops['value'], ';')[4], ',', '.').cast('DECIMAL(9,6)'))
df_bikeshops = df_bikeshops.withColumn('longitude', regexp_replace(split(df_bikeshops['value'], ';')[5], ',', '.').cast('DECIMAL(9,6)'))

# Drop the original 'value' column
df_bikeshops = df_bikeshops.drop('value')
df_bikeshops = df_bikeshops.drop('topic')






# Assuming df_orders is your DataFrame containing the 'value' column
df_orders = df_orders.withColumn('id', split(df_orders['value'], ';')[0])
df_orders = df_orders.withColumn('order_id', split(df_orders['value'], ';')[1].cast('INT'))
df_orders = df_orders.withColumn('order_line', split(df_orders['value'], ';')[2].cast('INT'))
df_orders = df_orders.withColumn('order_date', to_date(split(df_orders['value'], ';')[3],'M/d/yyyy'))
df_orders = df_orders.withColumn('customer_id', split(df_orders['value'], ';')[4].cast('INT'))
df_orders = df_orders.withColumn('product_id', split(df_orders['value'], ';')[5].cast('INT'))
df_orders = df_orders.withColumn('quantity', split(df_orders['value'], ';')[6].cast('INT'))

# Drop the original 'value' column
df_orders = df_orders.drop('value')
df_orders = df_orders.drop('topic')




# Await termination for each query


# query = table.writeStream.outputMode("append").format("console").start()

f = open("healthy", "w")
f.write("healthy")
f.close()






query_bikes = df_bikes.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_bikes") \
    .option("table", "bikes") \
    .option("keyspace", "default") \
    .start()


query_bikeshops = df_bikeshops.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_bikeshops") \
    .option("table", "bikeshops") \
    .option("keyspace", "default") \
    .start()

query_orders = df_orders.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint_orders") \
    .option("table", "orders") \
    .option("keyspace", "default") \
    .start()

query_bikes.awaitTermination()
query_bikeshops.awaitTermination()
query_orders.awaitTermination()