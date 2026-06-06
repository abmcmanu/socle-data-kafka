package io.socle.ingestion.api;

/** Réponse 202 : l'événement est durablement écrit dans Kafka (acks=all). */
public record OrderAcceptedResponse(String orderId, String eventId, String topic, int partition, long offset) {
}
