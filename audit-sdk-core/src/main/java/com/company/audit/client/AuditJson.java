package com.company.audit.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Builds the {@link ObjectMapper} used to serialize audit events to the Kafka wire format.
 *
 * <p>The wire contract the audit consumer relies on: {@code Instant} timestamps as ISO-8601
 * strings (not epoch millis), enums by name. There is no schema registry — this mapper's
 * configuration IS the contract, so it lives in the core and is used by every starter.
 */
public final class AuditJson {

    private AuditJson() {
    }

    public static ObjectMapper mapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
