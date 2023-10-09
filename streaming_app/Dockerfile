FROM spark:3.4.1-python3

WORKDIR /app

COPY . /app/



RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar

RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.4.1/spark-sql-streaming-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.4.1.jar


RUN curl https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar --output /opt/spark/jars/commons-pool2-2.11.1.jar

RUN curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar --output /opt/spark/jars/kafka-clients-3.4.1.jar 

RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar --output /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.4.1/spark-cassandra-connector_2.12-3.4.1.jar --output /opt/spark/jars/spark-cassandra-connector_2.12-3.4.1.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.4.1/spark-cassandra-connector-driver_2.12-3.4.1.jar --output /opt/spark/jars/spark-cassandra-connector-driver_2.12-3.4.1.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.13.0/java-driver-core-4.13.0.jar --output /opt/spark/jars/java-driver-core-4.13.0.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/oss/java-driver-mapper-processor/4.13.0/java-driver-mapper-processor-4.13.0.jar --output /opt/spark/jars/java-driver-mapper-processor-4.13.0.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/24.1-jre/java-driver-shaded-guava-24.1-jre.jar --output /opt/spark/jars/java-driver-shaded-guava-24.1-jre.jar

RUN curl https://repo1.maven.org/maven2/com/typesafe/config/1.4.0/config-1.4.0.jar --output /opt/spark/jars/config-1.4.0.jar

RUN curl https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.0/native-protocol-1.5.0.jar --output /opt/spark/jars/native-protocol-1.5.0.jar

RUN curl https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.9/jnr-posix-3.1.9.jar --output /opt/spark/jars/jnr-posix-3.1.9.jar

RUN curl https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.4/reactive-streams-1.0.4.jar --output /opt/spark/jars/reactive-streams-1.0.4.jar


# RUN mv /opt/spark/jars/spark-sql_2.12-3.5.0.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar

# RUN mv /opt/spark/jars/spark-streaming_2.12-3.5.0.jar /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar

# CMD ["/opt/spark/bin/spark-submit","KafkaStreamingToHDFS.jar"]


CMD ["/opt/spark/bin/spark-submit","app.py"]



# , "app.py" , "--packages" ,"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" , "org.apache.kafka:kafka-clients:3.5.1"