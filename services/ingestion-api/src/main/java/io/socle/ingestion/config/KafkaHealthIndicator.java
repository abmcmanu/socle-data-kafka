package io.socle.ingestion.config;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Santé de la connexion Kafka, exposée dans /actuator/health (sondée par blackbox-exporter).
 * Ne fait pas partie de la sonde de vivacité (liveness) : Kafka indisponible ne doit pas
 * provoquer le redémarrage de l'API, seulement son retrait du load balancer (readiness).
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final Admin admin;

    public KafkaHealthIndicator(Admin admin) {
        this.admin = admin;
    }

    @Override
    public Health health() {
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            int brokers = cluster.nodes().get(3, TimeUnit.SECONDS).size();
            String clusterId = cluster.clusterId().get(3, TimeUnit.SECONDS);
            return Health.up().withDetail("clusterId", clusterId).withDetail("brokers", brokers).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down(e).build();
        } catch (Exception e) {
            return Health.down().withDetail("error", e.getClass().getSimpleName()).build();
        }
    }
}
