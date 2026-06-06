package io.socle.ingestion.config;

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration Kafka de l'API ({@code socle.kafka.*} dans application.yml).
 *
 * @param bootstrapServers  liste des brokers
 * @param schemaRegistryUrl URL du Schema Registry
 * @param topic             topic de publication
 * @param sendTimeout       attente maximale de l'acquittement côté HTTP (≥ delivery.timeout.ms)
 * @param properties        propriétés client supplémentaires (sécurité SASL/TLS en production)
 */
@Validated
@ConfigurationProperties(prefix = "socle.kafka")
public record KafkaProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String schemaRegistryUrl,
        @NotBlank String topic,
        Duration sendTimeout,
        Map<String, String> properties) {

    public KafkaProperties {
        sendTimeout = sendTimeout == null ? Duration.ofSeconds(35) : sendTimeout;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }
}
