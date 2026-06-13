package io.socle.enrichment;

import io.socle.events.EnrichedOrder;
import io.socle.events.OrderPlaced;
import io.socle.events.RevenueByCountry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Topologie d'enrichissement :
 *
 * <pre>
 * orders.evt.order-placed.v1 ──► validation ──(invalide)──► orders.dlq.order-placed.v1 (+ en-tête dlq.reason)
 *                                    │
 *                                 (valide)
 *                                    ▼
 *                re-clé customerId ─► repartition « orders-by-customer »
 *                                    ▼
 *   crm.cdc.customers.v1 ─► KTable ─► leftJoin ─► re-clé orderId ─► orders.evt.order-enriched.v1
 *                                                       │
 *                                     groupBy pays:devise, fenêtre 1 min (grâce 30 s)
 *                                                       ▼
 *                         suppress(fin de fenêtre) ─► analytics.agg.revenue-by-country-1m.v1
 * </pre>
 *
 * Chaque opérateur porte un nom explicite : les topics et stores internes gardent un nom
 * stable quand la topologie évolue (sinon un redéploiement perdrait l'état).
 */
public final class EnrichmentTopology {

    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    public static final Duration WINDOW_GRACE = Duration.ofSeconds(30);

    private EnrichmentTopology() {
    }

    public static Topology build(AvroSerdes serdes, Clock clock, Properties props) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, OrderPlaced> orders = builder.stream(Topics.ORDERS_PLACED,
                Consumed.with(Serdes.String(), serdes.orderPublic())
                        .withTimestampExtractor(new OrderTimestampExtractor())
                        .withName("orders-source"));

        // 1. Validation : les commandes invalides sont isolées sans arrêter le flux.
        Map<String, KStream<String, OrderPlaced>> branches = orders
                .split(Named.as("validation-"))
                .branch((orderId, order) -> OrderValidator.validate(order).isPresent(), Branched.as("invalid"))
                .defaultBranch(Branched.as("valid"));

        branches.get("validation-invalid")
                .processValues(DlqReasonProcessor::new, Named.as("add-dlq-headers"))
                .to(Topics.ORDERS_DLQ, Produced.with(Serdes.String(), serdes.orderPublic()).withName("dlq-sink"));

        // 2. Table des clients : dernier état par id (topic compacté alimenté par Debezium).
        KTable<String, GenericRecord> customers = builder.table(Topics.CUSTOMERS,
                Consumed.with(Serdes.String(), serdes.customer()).withName("customers-source"),
                Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("customers-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serdes.customer()));

        // 3. Jointure : la commande doit être clé par customerId, comme la table (co-partitionnement).
        KStream<String, EnrichedOrder> enriched = branches.get("validation-valid")
                .selectKey((orderId, order) -> order.getCustomerId(), Named.as("rekey-by-customer"))
                .repartition(Repartitioned.<String, OrderPlaced>as("orders-by-customer")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serdes.orderInternal()))
                .leftJoin(customers,
                        (order, customer) -> Enricher.enrich(order, customer, clock.instant()),
                        Joined.<String, OrderPlaced, GenericRecord>as("join-customer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.orderInternal())
                                .withOtherValueSerde(serdes.customer()))
                .selectKey((customerId, order) -> order.getOrderId(), Named.as("rekey-by-order"));

        enriched.to(Topics.ORDERS_ENRICHED,
                Produced.with(Serdes.String(), serdes.enrichedPublic()).withName("enriched-sink"));

        // 4. Agrégat : CA et nombre de commandes par pays et devise, fenêtre fixe d'une minute.
        enriched
                .groupBy((orderId, order) -> Enricher.countryOf(order) + ":" + order.getCurrency(),
                        Grouped.with("revenue-by-country", Serdes.String(), serdes.enrichedInternal()))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE))
                .aggregate(EnrichmentTopology::emptyRevenue,
                        (key, order, agg) -> accumulate(order, agg),
                        Named.as("revenue-aggregate"),
                        Materialized.<String, RevenueByCountry, WindowStore<Bytes, byte[]>>as("revenue-by-country-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.revenueInternal()))
                // Un seul résultat définitif par fenêtre, émis après la période de grâce.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("revenue-final"))
                .toStream(Named.as("revenue-to-stream"))
                .map((windowedKey, agg) -> KeyValue.pair(windowedKey.key(), withWindow(agg, windowedKey.window())),
                        Named.as("set-window-bounds"))
                .to(Topics.REVENUE_BY_COUNTRY,
                        Produced.with(Serdes.String(), serdes.revenuePublic()).withName("revenue-sink"));

        return builder.build(props);
    }

    static RevenueByCountry emptyRevenue() {
        return RevenueByCountry.newBuilder()
                .setCountry("")
                .setCurrency("")
                .setWindowStart(Instant.EPOCH)
                .setWindowEnd(Instant.EPOCH)
                .setOrderCount(0L)
                .setRevenue(BigDecimal.ZERO.setScale(2, RoundingMode.UNNECESSARY))
                .build();
    }

    static RevenueByCountry accumulate(EnrichedOrder order, RevenueByCountry agg) {
        agg.setCountry(Enricher.countryOf(order));
        agg.setCurrency(order.getCurrency());
        agg.setOrderCount(agg.getOrderCount() + 1);
        agg.setRevenue(agg.getRevenue().add(order.getTotalAmount()).setScale(2, RoundingMode.HALF_UP));
        return agg;
    }

    static RevenueByCountry withWindow(RevenueByCountry agg, Window window) {
        agg.setWindowStart(window.startTime());
        agg.setWindowEnd(window.endTime());
        return agg;
    }
}
