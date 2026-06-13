package io.socle.enrichment;

import io.socle.events.OrderPlaced;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/** Ajoute les en-têtes de diagnostic avant l'écriture en DLQ (la clé et la valeur ne changent pas). */
public class DlqReasonProcessor implements FixedKeyProcessor<String, OrderPlaced, OrderPlaced> {

    public static final String REASON_HEADER = "dlq.reason";
    public static final String APP_HEADER = "dlq.application";

    private FixedKeyProcessorContext<String, OrderPlaced> context;

    @Override
    public void init(FixedKeyProcessorContext<String, OrderPlaced> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, OrderPlaced> record) {
        String reason = OrderValidator.validate(record.value()).orElse("unknown");
        record.headers().add(REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        record.headers().add(APP_HEADER, "order-enrichment".getBytes(StandardCharsets.UTF_8));
        context.forward(record);
    }
}
