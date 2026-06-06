package io.socle.ingestion.api;

import io.socle.ingestion.service.PublicationFailedException;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Traduit les échecs de publication en réponses RFC 9457 (application/problem+json). */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(PublicationFailedException.class)
    public ProblemDetail onPublicationFailed(PublicationFailedException e) {
        HttpStatus status = e.isRetriable() ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.INTERNAL_SERVER_ERROR;
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        problem.setType(URI.create("https://socle.io/problems/publication-failed"));
        problem.setProperty("eventId", e.eventId());
        problem.setProperty("retriable", e.isRetriable());
        return problem;
    }
}
