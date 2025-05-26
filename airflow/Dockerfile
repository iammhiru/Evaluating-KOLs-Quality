FROM apache/airflow:2.5.1-python3.9

USER root

# 1) Java & procps
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless procps wget && \
    rm -rf /var/lib/apt/lists/*

# 2) Spark client (để có spark-submit)
ARG SPARK_VERSION=3.3.2
ARG HADOOP_VERSION=3
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 3) Env cho Java & Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/usr/local/bin:${PATH}

# 4) Cài Spark Provider và pyspark cho Airflow
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark \
      pyspark

USER airflow
