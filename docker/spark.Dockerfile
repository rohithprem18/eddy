# syntax=docker/dockerfile:1.7
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Kafka source + Avro connectors, baked in so the job never resolves Maven at startup.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar /opt/spark/jars/

RUN python3 -m pip install --no-cache-dir redis==5.2.1 requests==2.32.3

WORKDIR /opt/eddy
COPY eddy ./eddy
COPY spark ./spark
RUN mkdir -p /opt/eddy/checkpoints && chown -R spark:spark /opt/eddy

ENV PYTHONPATH=/opt/eddy \
    PYSPARK_PYTHON=python3 \
    CHECKPOINT_DIR=/opt/eddy/checkpoints

USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--driver-memory", "2g", \
     "/opt/eddy/spark/feature_job.py"]
