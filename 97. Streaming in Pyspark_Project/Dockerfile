# Use a base image with Python and Java (required for Spark)
FROM openjdk:11-jre-slim

# Install Python and necessary dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    wget \
    tar \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
RUN pip3 install pyspark kafka-python

# Download and install Apache Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Install the Kafka-Spark connector (Required for Structured Streaming)
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar -P /opt/spark/jars/

# Download and install Kafka client JARs (Kafka required dependencies)
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar -P /opt/spark/jars/

# Download and install Apache Kafka
RUN wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz -P /opt \
    && tar -xzf /opt/kafka_2.12-2.8.0.tgz -C /opt \
    && ln -s /opt/kafka_2.12-2.8.0 /opt/kafka

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Add Kafka libraries to the Spark classpath
ENV SPARK_CLASSPATH=$SPARK_CLASSPATH:/opt/kafka/libs/*:/opt/spark/jars/*

# Set the working directory for the application
WORKDIR /app

# Copy your PySpark application into the container
COPY . /app

# Default command to run your PySpark script, including Kafka connector JAR and packages
CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.1.jar,/opt/spark/jars/kafka-clients-2.8.0.jar", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1", "/app/streaming_processor.py"]
