FROM bitnami/spark:3.3

USER root

RUN echo 'spark:x:1001:1001:Spark user:/home/spark:/bin/bash' >> /etc/passwd \
 && mkdir -p /home/spark \
 && chown 1001:1001 /home/spark

RUN mkdir -p /tmp/.ivy2 \
 && chown 1001:1001 /tmp/.ivy2

RUN mkdir -p $SPARK_HOME/conf \
 && echo "spark.jars.ivy /tmp/.ivy2" >> $SPARK_HOME/conf/spark-defaults.conf \
 && echo "spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,org.apache.iceberg:iceberg-hive-runtime:1.7.2" >> $SPARK_HOME/conf/spark-defaults.conf \
 && { \
      echo "spark.master                     spark://spark-master:7077"; \
      echo "spark.submit.deployMode          client"; \
      echo "spark.driver.python              /usr/bin/python3"; \
      echo "spark.executor.python            /usr/bin/python3"; \
    } >> $SPARK_HOME/conf/spark-defaults.conf

WORKDIR /opt/bitnami/spark
RUN mkdir -p ss \
    /opt/models/phobert-base-vi-sentiment-analysis \
    /opt/models/hf_cache

COPY batch/metric.py                    ss/metric.py
COPY batch/sentiment_analysis.py        ss/sentiment_analysis.py
COPY stream/comment.py                  ss/comment.py
COPY stream/post.py                     ss/post.py

RUN pip install \
      --extra-index-url https://download.pytorch.org/whl/cpu \
      torch==2.1.0+cpu \
      transformers \
      pandas \
      pyarrow \
      safetensors \
      huggingface_hub

RUN pip install "numpy<2"

RUN python3 -c 'from huggingface_hub import snapshot_download; \
snapshot_download( \
    repo_id="mr4/phobert-base-vi-sentiment-analysis", \
    local_dir="/opt/models/phobert-base-vi-sentiment-analysis", \
    cache_dir="/opt/models/hf_cache", \
    force_download=True \
)'

RUN chmod -R a+rX /opt/models/phobert-base-vi-sentiment-analysis && \
    chown -R 1001:1001    /opt/models/hf_cache

USER 1001

ENV TRANSFORMERS_CACHE=/opt/models/hf_cache

USER root

ARG AIRFLOW_VERSION=2.7.1
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip python3-venv curl \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m pip install --upgrade pip \
 && python3 -m pip install \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${CONSTRAINT_URL}" \
 && python3 -m pip install \
      "apache-airflow-providers-apache-spark" \
      --constraint "${CONSTRAINT_URL}" \
 && python3 -m pip install \
      --constraint "${CONSTRAINT_URL}" \
      "apache-airflow-providers-trino"

RUN python3 -m pip install --no-cache-dir psycopg2-binary

ENV AIRFLOW_HOME=/opt/airflow \
    SPARK_HOME=/opt/bitnami/spark
RUN mkdir -p /opt/airflow/dags \
             /opt/airflow/logs \
             /opt/airflow/plugins \
    && chown -R 1001:1001 /opt/airflow

COPY ./dags       $AIRFLOW_HOME/dags
COPY ./plugins    $AIRFLOW_HOME/plugins
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8085

ARG TRINO_VERSION=475
RUN mkdir -p /opt/trino \
 && curl -L \
    https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar \
    -o /opt/trino/trino \
 && chmod +x /opt/trino/trino

ENTRYPOINT [ "/opt/airflow/entrypoint.sh" ]

USER 1001