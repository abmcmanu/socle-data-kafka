package io.socle.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.socle.events.Channel;
import io.socle.events.EnrichedOrder;
import io.socle.events.OrderLine;
import io.socle.events.OrderPlaced;
import io.socle.events.RevenueByCountry;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests de la topologie sans broker (TopologyTestDriver) et sans Schema Registry
 * (registre en mémoire « mock:// »).
 */
class EnrichmentTopologyTest {

    private static final String SCOPE = "enrichment-test";
    private static final Instant T0 = Instant.parse("2026-11-03T10:00:05Z");
    private static final Instant NOW = Instant.parse("2026-11-03T10:00:06Z");

    /** Schéma équivalent à celui que Debezium produit pour public.customers (après unwrap). */
    private static final Schema CUSTOMER_SCHEMA = SchemaBuilder.record("Value").namespace("crm.public.customers")
            .fields()
            .requiredString("id")
            .requiredString("full_name")
            .requiredString("email")
            .requiredString("country")
            .requiredString("segment")
            .endRecord();

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderPlaced> orders;
    private TestInputTopic<String, GenericRecord> customers;
    private TestOutputTopic<String, EnrichedOrder> enriched;
    private TestOutputTopic<String, OrderPlaced> dlq;
    private TestOutputTopic<String, RevenueByCountry> revenue;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-enrichment-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        AvroSerdes serdes = AvroSerdes.create("mock://" + SCOPE, true, Map.of());
        driver = new TopologyTestDriver(
                EnrichmentTopology.build(serdes, Clock.fixed(NOW, ZoneOffset.UTC), props), props);

        orders = driver.createInputTopic(Topics.ORDERS_PLACED, new StringSerializer(), serdes.orderPublic().serializer());
        customers = driver.createInputTopic(Topics.CUSTOMERS, new StringSerializer(), serdes.customer().serializer());
        enriched = driver.createOutputTopic(Topics.ORDERS_ENRICHED, new StringDeserializer(), serdes.enrichedPublic().deserializer());
        dlq = driver.createOutputTopic(Topics.ORDERS_DLQ, new StringDeserializer(), serdes.orderPublic().deserializer());
        revenue = driver.createOutputTopic(Topics.REVENUE_BY_COUNTRY, new StringDeserializer(), serdes.revenuePublic().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    void enrichitUneCommandeAvecLeSegmentEtLePaysDuClient() {
        customers.pipeInput("C-0001", customer("C-0001", "FR", "PREMIUM"), T0.minusSeconds(60));

        orders.pipeInput("O-1", order("O-1", "C-0001", "19.90", 2, T0), T0);

        TestRecord<String, EnrichedOrder> out = enriched.readRecord();
        assertThat(out.key()).isEqualTo("O-1");
        assertThat(out.value().getCustomerSegment()).isEqualTo("PREMIUM");
        assertThat(out.value().getCustomerCountry()).isEqualTo("FR");
        assertThat(out.value().getItemCount()).isEqualTo(2);
        assertThat(out.value().getTotalAmount()).isEqualByComparingTo("39.80");
        assertThat(out.value().getEnrichedAt()).isEqualTo(NOW);
        assertThat(dlq.isEmpty()).isTrue();
    }

    @Test
    void clientInconnuDonneUnEnrichissementPartielSansBloquer() {
        orders.pipeInput("O-2", order("O-2", "C-9999", "10.00", 1, T0), T0);

        EnrichedOrder out = enriched.readValue();
        assertThat(out.getCustomerSegment()).isNull();
        assertThat(out.getCustomerCountry()).isNull();
    }

    @Test
    void commandeIncoherenteVaEnDlqAvecSaRaison() {
        OrderPlaced bad = order("O-3", "C-0001", "10.00", 1, T0);
        bad.setTotalAmount(new BigDecimal("99.00"));

        orders.pipeInput("O-3", bad, T0);

        TestRecord<String, OrderPlaced> rejected = dlq.readRecord();
        assertThat(rejected.key()).isEqualTo("O-3");
        assertThat(new String(rejected.headers().lastHeader(DlqReasonProcessor.REASON_HEADER).value(),
                StandardCharsets.UTF_8)).isEqualTo("total_mismatch");
        assertThat(enriched.isEmpty()).isTrue();
    }

    @Test
    void agregeLeChiffreDAffairesParPaysSurUneMinuteEtNEmetQuALaFermeture() {
        customers.pipeInput("C-0001", customer("C-0001", "FR", "PREMIUM"), T0.minusSeconds(60));
        customers.pipeInput("C-0002", customer("C-0002", "FR", "STANDARD"), T0.minusSeconds(60));

        orders.pipeInput("O-10", order("O-10", "C-0001", "10.00", 1, T0), T0);
        orders.pipeInput("O-11", order("O-11", "C-0002", "5.50", 2, T0.plusSeconds(20)), T0.plusSeconds(20));

        // Fenêtre [10:00, 10:01) encore ouverte (grâce de 30 s) : rien n'est émis.
        assertThat(revenue.isEmpty()).isTrue();

        // Une commande à 10:02 fait avancer le temps du flux au-delà de 10:01 + 30 s.
        orders.pipeInput("O-12", order("O-12", "C-0001", "1.00", 1, T0.plusSeconds(120)), T0.plusSeconds(120));

        TestRecord<String, RevenueByCountry> result = revenue.readRecord();
        assertThat(result.key()).isEqualTo("FR:EUR");
        assertThat(result.value().getOrderCount()).isEqualTo(2L);
        assertThat(result.value().getRevenue()).isEqualByComparingTo("21.00");
        assertThat(result.value().getWindowStart()).isEqualTo(Instant.parse("2026-11-03T10:00:00Z"));
        assertThat(result.value().getWindowEnd()).isEqualTo(Instant.parse("2026-11-03T10:01:00Z"));
        assertThat(revenue.isEmpty()).isTrue();
    }

    @Test
    void suppressionDuClientParTombstoneRetireLEnrichissement() {
        customers.pipeInput("C-0001", customer("C-0001", "FR", "VIP"), T0.minusSeconds(60));
        customers.pipeInput("C-0001", (GenericRecord) null, T0.minusSeconds(30));

        orders.pipeInput("O-20", order("O-20", "C-0001", "3.00", 1, T0), T0);

        assertThat(enriched.readValue().getCustomerSegment()).isNull();
    }

    private static GenericRecord customer(String id, String country, String segment) {
        GenericRecord r = new GenericData.Record(CUSTOMER_SCHEMA);
        r.put("id", id);
        r.put("full_name", "Client " + id);
        r.put("email", id.toLowerCase() + "@example.com");
        r.put("country", country);
        r.put("segment", segment);
        return r;
    }

    private static OrderPlaced order(String orderId, String customerId, String unitPrice, int quantity, Instant at) {
        BigDecimal price = new BigDecimal(unitPrice).setScale(2);
        return OrderPlaced.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setChannel(Channel.WEB)
                .setItems(List.of(OrderLine.newBuilder().setSku("SKU-1").setQuantity(quantity).setUnitPrice(price).build()))
                .setTotalAmount(price.multiply(BigDecimal.valueOf(quantity)))
                .setCurrency("EUR")
                .setPlacedAt(at)
                .build();
    }
}
