package io.socle.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import io.socle.events.Channel;
import io.socle.events.OrderLine;
import io.socle.events.OrderPlaced;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderValidatorTest {

    private static OrderPlaced.Builder valid() {
        return OrderPlaced.newBuilder()
                .setEventId("e-1")
                .setOrderId("O-1")
                .setCustomerId("C-0001")
                .setChannel(Channel.WEB)
                .setItems(List.of(OrderLine.newBuilder().setSku("A").setQuantity(2).setUnitPrice(new BigDecimal("5.00")).build()))
                .setTotalAmount(new BigDecimal("10.00"))
                .setCurrency("EUR")
                .setPlacedAt(Instant.EPOCH);
    }

    @Test
    void commandeValide() {
        assertThat(OrderValidator.validate(valid().build())).isEmpty();
    }

    @Test
    void sansArticle() {
        assertThat(OrderValidator.validate(valid().setItems(List.of()).build())).contains("no_items");
    }

    @Test
    void sansClient() {
        assertThat(OrderValidator.validate(valid().setCustomerId(" ").build())).contains("missing_customer");
    }

    @Test
    void totalIncoherent() {
        assertThat(OrderValidator.validate(valid().setTotalAmount(new BigDecimal("11.00")).build())).contains("total_mismatch");
    }

    @Test
    void quantiteNulle() {
        OrderPlaced order = valid()
                .setItems(List.of(OrderLine.newBuilder().setSku("A").setQuantity(0).setUnitPrice(new BigDecimal("5.00")).build()))
                .build();
        assertThat(OrderValidator.validate(order)).contains("non_positive_quantity");
    }
}
