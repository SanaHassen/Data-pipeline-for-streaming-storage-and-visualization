FROM spark:3.4.1-python3

WORKDIR /app

COPY . /app/



RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar

RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.4.1/spark-sql-streaming-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.4.1.jar


RUN curl https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar --output /opt/spark/jars/commons-pool2-2.11.1.jar


RUN curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar --output /opt/spark/jars/kafka-clients-3.4.1.jar 

RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar




# RUN mv /opt/spark/jars/spark-sql_2.12-3.5.0.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar

# RUN mv /opt/spark/jars/spark-streaming_2.12-3.5.0.jar /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar

# CMD ["/opt/spark/bin/spark-submit","KafkaStreamingToHDFS.jar"]


CMD ["/opt/spark/bin/spark-submit","app.py"]



# , "app.py" , "--packages" ,"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" , "org.apache.kafka:kafka-clients:3.5.1"