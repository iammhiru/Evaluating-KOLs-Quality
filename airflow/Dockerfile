# airflow/Dockerfile

# 1) Base trên spark-custom image (Iceberg + PhoBERT đã cài sẵn)
FROM hieupt_spark_fin:1.1

USER root

# 2) Cài Airflow core + Spark provider theo constraints chính thức
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
      --constraint "${CONSTRAINT_URL}"

RUN python3 -m pip install --no-cache-dir psycopg2-binary

# 3) Tạo cấu trúc thư mục Airflow
ENV AIRFLOW_HOME=/opt/airflow \
    SPARK_HOME=/opt/bitnami/spark
RUN mkdir -p /opt/airflow/dags \
             /opt/airflow/logs \
             /opt/airflow/plugins \
    && chown -R 1001:1001 /opt/airflow

# 4) Copy mã DAGs/plugins và entrypoint
COPY ./dags       $AIRFLOW_HOME/dags
COPY ./plugins    $AIRFLOW_HOME/plugins
# Copy entrypoint mới vào đúng chỗ
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8085

# Chuyển lại user airflow nếu cần
USER 1001

# Đặt entrypoint
ENTRYPOINT [ "/opt/airflow/entrypoint.sh" ]
CMD ["webserver"]
