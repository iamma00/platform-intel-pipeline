FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

COPY src/ /app/src/

# Postgres JDBC driver only — no S3/Hadoop-AWS jars needed
RUN curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER 185
