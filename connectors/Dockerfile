# Image Kafka Connect du socle : Debezium (CDC PostgreSQL) + HDFS Sink + convertisseur Avro.
# Les plugins sont téléchargés dans une étape séparée puis copiés : l'image finale ne contient
# ni curl ni unzip inutiles. Même liste de plugins que platform/strimzi/connect/kafka-connect.yaml.
FROM alpine:3.20 AS plugins
RUN apk add --no-cache curl unzip
ARG CONFLUENT_VERSION=8.0.0
ARG HDFS_CONNECTOR_VERSION=10.2.15
WORKDIR /plugins
RUN set -eux; \
    curl -fsSL -o hdfs.zip "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-hdfs/versions/${HDFS_CONNECTOR_VERSION}/confluentinc-kafka-connect-hdfs-${HDFS_CONNECTOR_VERSION}.zip"; \
    unzip -q hdfs.zip && rm hdfs.zip; \
    # Le zip du plugin n'embarque pas Guava, requis par hadoop-common (version de Hadoop 2.10.2).
    guava="confluentinc-kafka-connect-hdfs-${HDFS_CONNECTOR_VERSION}/lib/guava-11.0.2.jar"; \
    curl -fsSL -o "$guava" https://repo1.maven.org/maven2/com/google/guava/guava/11.0.2/guava-11.0.2.jar; \
    echo "35a3c69e19d72743cac83778aecbee68680f63eb  $guava" | sha1sum -c -; \
    curl -fsSL -o avro.zip "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CONFLUENT_VERSION}/confluentinc-kafka-connect-avro-converter-${CONFLUENT_VERSION}.zip"; \
    unzip -q avro.zip && rm avro.zip; \
    ls -1 /plugins

FROM quay.io/debezium/connect:3.2.0.Final
# Le connecteur PostgreSQL est déjà présent dans l'image Debezium (/kafka/connect/debezium-connector-postgres).
COPY --from=plugins --chown=kafka:kafka /plugins/ /kafka/connect/
LABEL org.opencontainers.image.title="socle-kafka-connect" \
      org.opencontainers.image.description="Kafka Connect : Debezium PostgreSQL, HDFS Sink, Avro"
