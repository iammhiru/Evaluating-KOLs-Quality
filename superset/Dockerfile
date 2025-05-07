FROM apachesuperset.docker.scarf.sh/apache/superset:latest
USER root
RUN pip install psycopg2-binary
RUN pip install --no-cache-dir "trino[sqlalchemy]>=0.320.0"
USER superset
