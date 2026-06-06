package io.socle.ingestion.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.socle.events.Channel;
import io.socle.events.OrderLine;
import io.socle.events.OrderPlaced;
import io.socle.ingestion.api.OrderAcceptedResponse;
import io.socle.ingestion.api.OrderRequest;
import io.socle.ingestion.config.KafkaProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Transforme une requête en événement {@link OrderPlaced} et le publie.
 *
 * <ul>
 *   <li>Clé = orderId : tous les événements d'une commande vont dans la même partition (ordre garanti).</li>
 *   <li>Timestamp du record = placedAt : Kafka Streams travaille en event time.</li>
 *   <li>L'appel attend l'acquittement : un 202 signifie « écrit sur au moins 2 réplicas ».</li>
 * </ul>
 */
@Service
public class OrderPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(OrderPublisher.class);

    private final Producer<String, OrderPlaced> producer;
    private final KafkaProperties props;
    private final Clock clock;
    private final Counter published;
    private final Counter failed;

    @Autowired
    public OrderPublisher(Producer<String, OrderPlaced> producer, KafkaProperties props, MeterRegistry registry) {
        this(producer, props, registry, Clock.systemUTC());
    }

    OrderPublisher(Producer<String, OrderPlaced> producer, KafkaProperties props, MeterRegistry registry, Clock clock) {
        this.producer = producer;
        this.props = props;
        this.clock = clock;
        this.published = Counter.builder("socle.orders.published").description("Commandes acquittées par Kafka").register(registry);
        this.failed = Counter.builder("socle.orders.publish.failed").description("Commandes non acquittées").register(registry);
    }

    public OrderAcceptedResponse publish(OrderRequest request) {
        OrderPlaced event = toEvent(request);
        ProducerRecord<String, OrderPlaced> record = new ProducerRecord<>(
                props.topic(), null, event.getPlacedAt().toEpochMilli(), event.getOrderId(), event);
        record.headers().add(new RecordHeader("source", "ingestion-api".getBytes(StandardCharsets.UTF_8)));

        try {
            RecordMetadata md = producer.send(record).get(props.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            published.increment();
            LOG.debug("Commande {} écrite en {}-{}@{}", event.getOrderId(), md.topic(), md.partition(), md.offset());
            return new OrderAcceptedResponse(event.getOrderId(), event.getEventId(), md.topic(), md.partition(), md.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failed.increment();
            throw new PublicationFailedException("Publication interrompue", event.getEventId(), true, e);
        } catch (TimeoutException e) {
            failed.increment();
            throw new PublicationFailedException("Kafka n'a pas acquitté dans le délai", event.getEventId(), true, e);
        } catch (ExecutionException e) {
            failed.increment();
            Throwable cause = e.getCause();
            // Après delivery.timeout.ms, une erreur retriable (NotEnoughReplicas, timeout...) remonte ici.
            boolean retriable = cause instanceof RetriableException
                    || cause instanceof org.apache.kafka.common.errors.TimeoutException;
            LOG.warn("Échec de publication de la commande {} : {}", event.getOrderId(), cause.toString());
            throw new PublicationFailedException("Échec de publication : " + cause.getClass().getSimpleName(),
                    event.getEventId(), retriable, cause);
        }
    }

    OrderPlaced toEvent(OrderRequest request) {
        List<OrderLine> lines = request.items().stream()
                .map(l -> OrderLine.newBuilder()
                        .setSku(l.sku())
                        .setQuantity(l.quantity())
                        .setUnitPrice(scale(l.unitPrice()))
                        .build())
                .toList();
        BigDecimal total = request.items().stream()
                .map(l -> l.unitPrice().multiply(BigDecimal.valueOf(l.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        Instant placedAt = (request.placedAt() != null ? request.placedAt() : clock.instant())
                .truncatedTo(ChronoUnit.MILLIS);
        return OrderPlaced.newBuilder()
                .setEventId(request.eventId() != null ? request.eventId().toLowerCase() : UUID.randomUUID().toString())
                .setOrderId(request.orderId() != null ? request.orderId() : "O-" + UUID.randomUUID())
                .setCustomerId(request.customerId())
                .setChannel(Channel.valueOf(request.channel().name()))
                .setItems(lines)
                .setTotalAmount(scale(total))
                .setCurrency(request.currency())
                .setPlacedAt(placedAt)
                .build();
    }

    /** Le type Avro decimal(12,2) exige exactement 2 décimales. */
    private static BigDecimal scale(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }
}
