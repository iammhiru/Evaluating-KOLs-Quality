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

COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

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

ENTRYPOINT ["/start-spark.sh"]
