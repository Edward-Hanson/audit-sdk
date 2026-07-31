package com.company.audit.exception;

/**
 * Thrown when an AuditEvent fails SDK-side validation, before it is sent to Kafka.
 * The message lists exactly which fields are invalid so the developer gets clear feedback.
 */
public class AuditValidationException extends RuntimeException {

    public AuditValidationException(String message) {
        super(message);
    }
}
