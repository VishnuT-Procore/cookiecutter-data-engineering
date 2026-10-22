# Flink 1.20 (LTS) with the Kafka and JDBC connectors. Jars are fetched (and checksum-verified)
# on the host by flink/fetch_jars.py, so the image build needs no network access.
FROM flink:1.20.3-scala_2.12-java17
COPY lib/*.jar /opt/flink/lib/
