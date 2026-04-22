FROM eclipse-temurin:11-jdk-jammy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

RUN mkdir -p /opt/spark/jars

RUN curl -L -o /opt/spark/jars/gcs-connector-hadoop3-2.2.24.jar \
https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.24.jar

RUN curl -L -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar

COPY clean_data_and_bigquery_ingest.py /app/

ENTRYPOINT ["uv", "run", "python", "clean_data_and_bigquery_ingest.py"]