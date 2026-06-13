package io.socle.enrichment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application Kafka Streams d'enrichissement des commandes (exactly-once).
 *
 * <p>Configuration par variables d'environnement :
 * KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, STATE_DIR, APPLICATION_ID,
 * KAFKA_CLIENT_CONFIG (fichier .properties optionnel : sécurité SASL/TLS en production).
 */
public final class OrderEnrichmentApp {

    private static final Logger LOG = LoggerFactory.getLogger(OrderEnrichmentApp.class);

    private OrderEnrichmentApp() {
    }

    public static void main(String[] args) throws Exception {
        Properties props = streamsConfig(System.getenv());
        AvroSerdes serdes = AvroSerdes.create(
                env(System.getenv(), "SCHEMA_REGISTRY_URL", "http://localhost:8081"), false, securityProps(props));
        Topology topology = EnrichmentTopology.build(serdes, Clock.systemUTC(), props);
        LOG.info("Topologie :\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch stopped = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) -> {
            LOG.info("État Kafka Streams : {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.NOT_RUNNING) {
                stopped.countDown();
            }
        });
        // Une exception non rattrapée arrête ce client : l'orchestrateur (Docker, systemd, Kubernetes)
        // le relance, et la reprise se fait depuis les derniers offsets validés (exactly-once).
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("Erreur fatale dans un thread Streams", e);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        // Arrêt propre (SIGTERM) : termine la transaction en cours et quitte le groupe.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Arrêt demandé : fermeture de Kafka Streams");
            streams.close(Duration.ofSeconds(30));
            stopped.countDown();
        }, "shutdown-hook"));

        streams.start();
        stopped.await();
        System.exit(streams.state() == KafkaStreams.State.ERROR ? 1 : 0);
    }

    static Properties streamsConfig(Map<String, String> env) throws IOException {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, env(env, "APPLICATION_ID", "order-enrichment"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        p.put(StreamsConfig.STATE_DIR_CONFIG, env(env, "STATE_DIR", "/tmp/order-enrichment"));
        // Exactly-once : transactions + lecture read_committed (ADR 0005).
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Topics internes répliqués comme les topics métier.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        p.put(StreamsConfig.topicPrefix("min.insync.replicas"), "2");
        // Une copie chaude des state stores sur une autre instance : bascule rapide.
        p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.parseInt(env(env, "STREAM_THREADS", "2")));
        // Réutilise le topic source comme changelog de la KTable clients (pas de topic en double).
        p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        // Un message illisible est journalisé et compté (métrique dropped-records), sans arrêter le flux.
        p.put("deserialization.exception.handler", LogAndContinueExceptionHandler.class.getName());
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        p.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");
        p.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 20);

        String clientConfig = env.get("KAFKA_CLIENT_CONFIG");
        if (clientConfig != null && !clientConfig.isBlank()) {
            try (InputStream in = Files.newInputStream(Path.of(clientConfig))) {
                Properties security = new Properties();
                security.load(in);
                p.putAll(security);
            }
        }
        return p;
    }

    /** Propriétés de sécurité à transmettre aussi au client du Schema Registry. */
    private static Map<String, String> securityProps(Properties props) {
        Map<String, String> out = new HashMap<>();
        props.stringPropertyNames().stream()
                .filter(k -> k.startsWith("schema.registry.") || k.startsWith("basic.auth."))
                .forEach(k -> out.put(k, props.getProperty(k)));
        return out;
    }

    private static String env(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
