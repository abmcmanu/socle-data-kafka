package io.socle.enrichment;

import io.socle.events.OrderPlaced;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event time = placedAt de la commande, et non l'heure d'écriture dans Kafka.
 * Les fenêtres d'une minute reflètent donc le moment réel de la vente, même si un
 * producteur publie en retard. Repli sur le timestamp du record si la valeur est illisible.
 */
public class OrderTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof OrderPlaced order && order.getPlacedAt() != null) {
            return order.getPlacedAt().toEpochMilli();
        }
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}
