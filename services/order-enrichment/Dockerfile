# Construire d'abord le jar : mvn -f services/pom.xml package
FROM eclipse-temurin:21-jre-alpine
# libstdc++ : requise par la bibliothèque native de RocksDB (state stores Kafka Streams), absente d'Alpine.
RUN apk add --no-cache libstdc++ \
  && addgroup -S app && adduser -S app -G app && mkdir -p /var/lib/order-enrichment && chown app:app /var/lib/order-enrichment
WORKDIR /app
COPY target/order-enrichment.jar app.jar
USER app
# Le state store RocksDB vit dans STATE_DIR : un volume persistant évite une restauration complète au redémarrage.
VOLUME ["/var/lib/order-enrichment"]
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
