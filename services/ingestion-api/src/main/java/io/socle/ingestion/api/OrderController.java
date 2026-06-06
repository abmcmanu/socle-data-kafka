package io.socle.ingestion.api;

import io.socle.ingestion.service.OrderPublisher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderPublisher publisher;

    public OrderController(OrderPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    @Operation(summary = "Publie une commande sur orders.evt.order-placed.v1")
    @ApiResponse(responseCode = "202", description = "Commande écrite et répliquée dans Kafka")
    @ApiResponse(responseCode = "400", description = "Requête invalide, rien n'est publié")
    @ApiResponse(responseCode = "503", description = "Kafka n'a pas acquitté dans le délai ; le client peut réessayer avec le même eventId")
    public ResponseEntity<OrderAcceptedResponse> place(@Valid @RequestBody OrderRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(publisher.publish(request));
    }
}
