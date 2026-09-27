FROM apache/spark:3.5.0

# Kafka connector jars baked in at build time so spark-submit needs no
# --packages resolution (and no Maven access) at runtime.
USER root
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    curl -sSL -o spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar; \
    curl -sSL -o kafka-clients-3.6.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar; \
    curl -sSL -o spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

COPY job.py transformations.py /opt/app/
WORKDIR /opt/app

ENV KAFKA_BOOTSTRAP=redpanda:29092 \
    TOPIC=clickstream-events \
    SERVING_DIR=/opt/app/data/serving \
    LAKE_DIR=/opt/app/data/lake \
    CHECKPOINT_DIR=/opt/app/data/checkpoints

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "2g", "/opt/app/job.py"]
