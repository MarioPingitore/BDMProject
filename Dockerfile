# Use a base Ubuntu image
FROM ubuntu:20.04

# Set environment variables for Python, Spark, and Hadoop versions
ENV PYTHON_VERSION=3.8
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3.3

# Install dependencies
RUN apt-get update && \
    apt-get install -y wget openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install Python
RUN apt-get update && \
    apt-get install -y python${PYTHON_VERSION} python3-pip && \
    ln -s /usr/bin/python${PYTHON_VERSION} /usr/bin/python3 && \
    ln -s /usr/bin/pip${PYTHON_VERSION} /usr/bin/pip && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables for Spark and PySpark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3


# Install pip requirements
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

# Set environment variables for MongoDB connection
ENV MONGO_URI="mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject"
ENV MONGO_DB="BDMProject"

# Set the working directory
WORKDIR /app

# Copy the application code to the container
COPY . /app

# Set the entry point to run the PySpark application
ENTRYPOINT ["python", "/app/Main.py"]
