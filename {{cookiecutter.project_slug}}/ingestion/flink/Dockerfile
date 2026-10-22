# Flink 1.20 (LTS) with Kafka + Iceberg (REST catalog, S3FileIO). Jars are fetched and checksum-verified
# on the host by fetch_jars.py, so the image build itself needs no network access.
FROM flink:1.20.3-scala_2.12-java17
COPY lib/*.jar /opt/flink/lib/
