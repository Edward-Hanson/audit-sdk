package com.company.audit.registration;

/**
 * Thrown when the one-time startup registration with the audit service fails — either
 * obtaining the Entra token or the {@code POST /register} call. Because registration is
 * mandatory when auditing is enabled, this propagates out of the startup runner and the
 * application fails to start.
 */
public class AuditRegistrationException extends RuntimeException {

    public AuditRegistrationException(String message) {
        super(message);
    }

    public AuditRegistrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
