package io.socle.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Point d'entrée REST des commandes. Valide la requête, la transforme en événement Avro
 * {@code OrderPlaced} et la publie sur {@code orders.evt.order-placed.v1} avec un producer
 * idempotent ({@code acks=all}). La réponse 202 n'est renvoyée qu'après l'acquittement de Kafka.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class IngestionApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionApiApplication.class, args);
    }
}
