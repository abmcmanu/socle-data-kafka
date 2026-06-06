package io.socle.ingestion.service;

/** Kafka n'a pas acquitté l'événement. {@code retriable} : le client peut réessayer avec le même eventId. */
public class PublicationFailedException extends RuntimeException {

    private final String eventId;
    private final boolean retriable;

    public PublicationFailedException(String message, String eventId, boolean retriable, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
        this.retriable = retriable;
    }

    public String eventId() {
        return eventId;
    }

    public boolean isRetriable() {
        return retriable;
    }
}
