package com.company.audit.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The audit event sent to Kafka.
 *
 * NOTE: this is intentionally NOT the JPA entity. It carries no persistence
 * concerns (no @Id, no generated id, no Hibernate annotations). The audit
 * service maps this into its own entity when it stores the event.
 *
 * Validation is done by the SDK (Bean Validation annotations below) before
 * the event is serialized and sent. There is no external schema registry.
 */
public class AuditEvent {

    /** Client-generated unique id. Used by the consumer to deduplicate. */
    @NotBlank
    private String eventId = UUID.randomUUID().toString();

    /** Which application emitted this event (payroll, loans, employee, ...). */
    @NotBlank
    private String sourceService;

    @NotBlank
    private String userName;

    @NotNull
    private Long userId;

    @NotBlank
    private String entityId;

    @NotBlank
    private String entityType;

    private String entityName;

    @NotBlank
    private String action;

    private String details;

    @NotNull
    private Integer organizationId;

    private Map<String, Object> currentPayload;

    private Map<String, Object> payload;

    private Map<String, Object> changedPayload;

    /** Event time (UTC instant). Defaults to now if not set. */
    @NotNull
    private Instant timestamp = Instant.now();

    public AuditEvent() {
    }

    // --- getters / setters ---

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSourceService() { return sourceService; }
    public void setSourceService(String sourceService) { this.sourceService = sourceService; }

    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }

    public String getEntityId() { return entityId; }
    public void setEntityId(String entityId) { this.entityId = entityId; }

    public String getEntityType() { return entityType; }
    public void setEntityType(String entityType) { this.entityType = entityType; }

    public String getEntityName() { return entityName; }
    public void setEntityName(String entityName) { this.entityName = entityName; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }

    public Integer getOrganizationId() { return organizationId; }
    public void setOrganizationId(Integer organizationId) { this.organizationId = organizationId; }

    public Map<String, Object> getCurrentPayload() { return currentPayload; }
    public void setCurrentPayload(Map<String, Object> currentPayload) { this.currentPayload = currentPayload; }

    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    public Map<String, Object> getChangedPayload() { return changedPayload; }
    public void setChangedPayload(Map<String, Object> changedPayload) { this.changedPayload = changedPayload; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
