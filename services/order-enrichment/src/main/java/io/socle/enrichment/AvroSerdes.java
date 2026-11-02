package io.socle.enrichment;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.socle.events.EnrichedOrder;
import io.socle.events.OrderPlaced;
import io.socle.events.RevenueByCountry;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/**
 * Serdes Avro de l'application, en deux familles :
 * <ul>
 *   <li><b>publics</b> : topics gouvernés ; les schémas sont enregistrés par la CI,
 *       donc {@code auto.register.schemas=false} (un schéma non validé est refusé) ;</li>
 *   <li><b>internes</b> : topics de repartitionnement et changelogs créés par Kafka Streams
 *       ({@code order-enrichment-*}) ; leurs subjects n'existent pas à l'avance, l'application
 *       les enregistre elle-même.</li>
 * </ul>
 */
public record AvroSerdes(
        Serde<OrderPlaced> orderPublic,
        Serde<OrderPlaced> orderInternal,
        Serde<EnrichedOrder> enrichedPublic,
        Serde<EnrichedOrder> enrichedInternal,
        Serde<RevenueByCountry> revenuePublic,
        Serde<RevenueByCountry> revenueInternal,
        Serde<GenericRecord> customer) {

    public static AvroSerdes create(String schemaRegistryUrl, boolean autoRegisterPublic, Map<String, String> extra) {
        return new AvroSerdes(
                specific(schemaRegistryUrl, autoRegisterPublic, extra),
                specific(schemaRegistryUrl, true, extra),
                specific(schemaRegistryUrl, autoRegisterPublic, extra),
                specific(schemaRegistryUrl, true, extra),
                specific(schemaRegistryUrl, autoRegisterPublic, extra),
                specific(schemaRegistryUrl, true, extra),
                generic(schemaRegistryUrl, extra));
    }

    private static Map<String, Object> config(String url, boolean autoRegister, Map<String, String> extra) {
        Map<String, Object> cfg = new HashMap<>(extra);
        cfg.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
        cfg.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegister);
        return cfg;
    }

    private static <T extends SpecificRecord> Serde<T> specific(String url, boolean autoRegister, Map<String, String> extra) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(config(url, autoRegister, extra), false);
        return serde;
    }

    private static Serde<GenericRecord> generic(String url, Map<String, String> extra) {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(config(url, true, extra), false);
        return serde;
    }
}
