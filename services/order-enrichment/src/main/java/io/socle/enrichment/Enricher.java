package io.socle.enrichment;

import io.socle.events.EnrichedOrder;
import io.socle.events.OrderPlaced;
import java.time.Instant;
import org.apache.avro.generic.GenericRecord;

/** Fusionne une commande et l'état du client (peut être null : client inconnu du CRM). */
public final class Enricher {

    public static final String UNKNOWN_COUNTRY = "UNKNOWN";

    private Enricher() {
    }

    public static EnrichedOrder enrich(OrderPlaced order, GenericRecord customer, Instant now) {
        return EnrichedOrder.newBuilder()
                .setEventId(order.getEventId())
                .setOrderId(order.getOrderId())
                .setCustomerId(order.getCustomerId())
                .setChannel(order.getChannel())
                .setItemCount(order.getItems().stream().mapToInt(l -> l.getQuantity()).sum())
                .setTotalAmount(order.getTotalAmount())
                .setCurrency(order.getCurrency())
                .setPlacedAt(order.getPlacedAt())
                .setCustomerSegment(field(customer, "segment"))
                .setCustomerCountry(field(customer, "country"))
                .setEnrichedAt(now)
                .build();
    }

    public static String countryOf(EnrichedOrder order) {
        return order.getCustomerCountry() != null ? order.getCustomerCountry() : UNKNOWN_COUNTRY;
    }

    /** Debezium produit des chaînes Avro (Utf8) : on les convertit en String. */
    private static String field(GenericRecord record, String name) {
        if (record == null || record.getSchema().getField(name) == null) {
            return null;
        }
        Object value = record.get(name);
        return value == null ? null : value.toString().trim();
    }
}
