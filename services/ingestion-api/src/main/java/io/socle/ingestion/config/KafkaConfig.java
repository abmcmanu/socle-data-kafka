package io.socle.ingestion.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.socle.events.OrderPlaced;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Producer orienté durabilité, voir docs/architecture/02-flux-de-donnees.md.
 */
@Configuration
public class KafkaConfig {

    @Bean(destroyMethod = "close")
    public Producer<String, OrderPlaced> orderProducer(KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "ingestion-api");
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // Avec min.insync.replicas=2 côté topic.
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        // Au-delà, l'API répond 503.
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        // Les schémas sont enregistrés par la CI (ADR 0002).
        cfg.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.schemaRegistryUrl());
        cfg.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        cfg.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false);
        // security.*, sasl.*, ssl.* en production
        cfg.putAll(props.properties());
        return new KafkaProducer<>(cfg);
    }

    @Bean(destroyMethod = "close")
    public Admin kafkaAdmin(KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        cfg.put(AdminClientConfig.CLIENT_ID_CONFIG, "ingestion-api-health");
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        props.properties().forEach((k, v) -> {
            if (k.startsWith("security.") || k.startsWith("sasl.") || k.startsWith("ssl.")) {
                cfg.put(k, v);
            }
        });
        return Admin.create(cfg);
    }
}
