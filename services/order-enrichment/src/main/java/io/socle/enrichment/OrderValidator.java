package io.socle.enrichment;

import io.socle.events.OrderLine;
import io.socle.events.OrderPlaced;
import java.math.BigDecimal;
import java.util.Optional;

/**
 * Règles métier appliquées à chaque commande. Défense en profondeur : l'API valide déjà,
 * mais d'autres producteurs peuvent écrire sur le topic. Une commande invalide part en DLQ
 * au lieu de bloquer le traitement (pas de « poison pill »).
 */
public final class OrderValidator {

    private OrderValidator() {
    }

    /** @return la raison du rejet, ou vide si la commande est valide */
    public static Optional<String> validate(OrderPlaced order) {
        if (order == null) {
            return Optional.of("null_value");
        }
        if (order.getCustomerId() == null || order.getCustomerId().isBlank()) {
            return Optional.of("missing_customer");
        }
        if (order.getItems() == null || order.getItems().isEmpty()) {
            return Optional.of("no_items");
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (OrderLine line : order.getItems()) {
            if (line.getQuantity() <= 0) {
                return Optional.of("non_positive_quantity");
            }
            sum = sum.add(line.getUnitPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
        }
        if (order.getTotalAmount().signum() <= 0) {
            return Optional.of("non_positive_total");
        }
        if (sum.compareTo(order.getTotalAmount()) != 0) {
            return Optional.of("total_mismatch");
        }
        return Optional.empty();
    }
}
