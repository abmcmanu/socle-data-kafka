package io.socle.ingestion.api;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Corps de POST /api/v1/orders. */
public record OrderRequest(
        @Schema(description = "Identifiant de la commande ; généré si absent", example = "O-2026-000123")
        @Size(max = 64) String orderId,
        @Schema(description = "Identifiant fourni par le client pour rendre ses retries idempotents (UUID)")
        @Pattern(regexp = "^[0-9a-fA-F-]{36}$") String eventId,
        @Schema(example = "C-0001") @NotBlank @Size(max = 16) String customerId,
        @Schema(example = "WEB", allowableValues = {"WEB", "MOBILE", "STORE"}) @NotNull ChannelDto channel,
        @Schema(example = "EUR") @NotBlank @Pattern(regexp = "^[A-Z]{3}$") String currency,
        @Schema(description = "Heure de la commande ; heure de réception si absente") Instant placedAt,
        @NotEmpty @Size(max = 200) List<@Valid Line> items) {

    public enum ChannelDto { WEB, MOBILE, STORE }

    public record Line(
            @Schema(example = "SKU-42") @NotBlank @Size(max = 64) String sku,
            @Schema(example = "2") @Positive int quantity,
            @Schema(example = "19.90") @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2)
            BigDecimal unitPrice) {
    }
}
