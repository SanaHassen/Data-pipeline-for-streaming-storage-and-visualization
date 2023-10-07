from pyspark import *
from pyspark.sql import *
from pyspark.sql.utils import *
from pyspark.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import *



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



def foreachBatch_function(batch_df, batch_id) :
  for row in batch_df.collect():
            topic = row['topic']
            value = row['value']
            with open(f"hdfs://hdfs-namenode/data/{topic}.csv", "a") as file:
                file.write(value + '\n')

# Define a streaming query to write to HDFS
query = df.writeStream \
  .outputMode("append") \
  .format("csv") \
  .option("path", "hdfs://hdfs-namenode/data/result") \
  .start()



# query = table.writeStream.outputMode("append").format("console").start()

# query = table.writeStream.outputMode("append").format("console").start()
query.awaitTermination()