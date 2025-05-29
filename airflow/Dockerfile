FROM hieupt_spark_fin:1.1

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
      --constraint "${CONSTRAINT_URL}"

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

USER 1001

ENTRYPOINT [ "/opt/airflow/entrypoint.sh" ]
