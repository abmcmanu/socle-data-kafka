package io.socle.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.socle.events.OrderPlaced;
import io.socle.ingestion.api.OrderRequest;
import io.socle.ingestion.api.OrderRequest.ChannelDto;
import io.socle.ingestion.api.OrderRequest.Line;
import io.socle.ingestion.config.KafkaProperties;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrderPublisherTest {

    private static final Instant NOW = Instant.parse("2026-11-03T10:15:30.123456Z");
    private static final String TOPIC = "orders.evt.order-placed.v1";

    private MockProducer<String, OrderPlaced> producer;
    private SimpleMeterRegistry registry;
    private OrderPublisher publisher;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(false, null, new StringSerializer(), (topic, value) -> new byte[0]);
        registry = new SimpleMeterRegistry();
        KafkaProperties props = new KafkaProperties("unused:9092", "mock://test", TOPIC, Duration.ofSeconds(2), Map.of());
        publisher = new OrderPublisher(producer, props, registry, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static OrderRequest request(String orderId) {
        return new OrderRequest(orderId, null, "C-0001", ChannelDto.WEB, "EUR", null, List.of(
                new Line("SKU-1", 2, new BigDecimal("19.90")),
                new Line("SKU-2", 1, new BigDecimal("5"))));
    }

    @Test
    void publieAvecLaCleOrderIdEtLeTimestampDeLEvenement() {
        Thread acker = new Thread(() -> {
            while (!producer.completeNext()) {
                Thread.onSpinWait();
            }
        });
        acker.start();

        var response = publisher.publish(request("O-1"));

        assertThat(response.orderId()).isEqualTo("O-1");
        ProducerRecord<String, OrderPlaced> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(TOPIC);
        assertThat(sent.key()).isEqualTo("O-1");
        assertThat(sent.timestamp()).isEqualTo(NOW.toEpochMilli());
        assertThat(sent.headers().lastHeader("source")).isNotNull();
        assertThat(registry.counter("socle.orders.published").count()).isEqualTo(1.0);
    }

    @Test
    void calculeLeTotalAvecDeuxDecimalesExactement() {
        OrderPlaced event = publisher.toEvent(request("O-2"));

        assertThat(event.getTotalAmount()).isEqualByComparingTo("44.80");
        assertThat(event.getTotalAmount().scale()).isEqualTo(2);
        assertThat(event.getItems().get(1).getUnitPrice().scale()).isEqualTo(2);
        assertThat(event.getPlacedAt()).isEqualTo(Instant.parse("2026-11-03T10:15:30.123Z"));
        assertThat(event.getEventId()).hasSize(36);
    }

    @Test
    void erreurRetriableDeKafkaDevientUne503() {
        Thread failer = new Thread(() -> {
            while (!producer.errorNext(new NotEnoughReplicasException("ISR=1 < min.insync.replicas=2"))) {
                Thread.onSpinWait();
            }
        });
        failer.start();

        assertThatThrownBy(() -> publisher.publish(request("O-3")))
                .isInstanceOf(PublicationFailedException.class)
                .satisfies(e -> assertThat(((PublicationFailedException) e).isRetriable()).isTrue());
        assertThat(registry.counter("socle.orders.publish.failed").count()).isEqualTo(1.0);
    }
}
