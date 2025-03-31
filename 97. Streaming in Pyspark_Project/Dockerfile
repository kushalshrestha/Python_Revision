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
# Download and install Apache Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz


# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory for the application
WORKDIR /app

# Copy your PySpark application into the container
COPY . /app

# Default command to run your PySpark script
CMD ["spark-submit", "/app/streaming_processor.py"]
