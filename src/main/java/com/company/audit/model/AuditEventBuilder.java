package com.company.audit.model;

import java.time.Instant;
import java.util.Map;

/**
 * Fluent builder for AuditEvent so calling code stays readable.
 * sourceService is injected by the SDK from config, so teams don't set it.
 */
public final class AuditEventBuilder {

    private final AuditEvent event = new AuditEvent();

    public static AuditEventBuilder builder() {
        return new AuditEventBuilder();
    }

    public AuditEventBuilder userName(String v) { event.setUserName(v); return this; }
    public AuditEventBuilder userId(Long v) { event.setUserId(v); return this; }
    public AuditEventBuilder entityId(String v) { event.setEntityId(v); return this; }
    public AuditEventBuilder entityType(String v) { event.setEntityType(v); return this; }
    public AuditEventBuilder entityName(String v) { event.setEntityName(v); return this; }
    public AuditEventBuilder action(String v) { event.setAction(v); return this; }
    public AuditEventBuilder details(String v) { event.setDetails(v); return this; }
    public AuditEventBuilder organizationId(Integer v) { event.setOrganizationId(v); return this; }
    public AuditEventBuilder currentPayload(Map<String, Object> v) { event.setCurrentPayload(v); return this; }
    public AuditEventBuilder payload(Map<String, Object> v) { event.setPayload(v); return this; }
    public AuditEventBuilder changedPayload(Map<String, Object> v) { event.setChangedPayload(v); return this; }
    public AuditEventBuilder timestamp(Instant v) { event.setTimestamp(v); return this; }

    public AuditEvent build() {
        return event;
    }
}
