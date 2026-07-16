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

    /** Emitting application's display name (from {@code audit.display-name}). SDK-stamped. */
    @NotBlank
    private String sourceService;

    /** Verified Entra client id (from {@code entra.client-id}). SDK-stamped; used to attribute the event. */
    @NotBlank
    private String clientId;

    @NotBlank
    private String userName;

    @NotNull
    private Long userId;

    @NotBlank
    private String entityId;

    @NotBlank
    private String entityType;

    private String entityName;

    /** What happened. Restricted to the {@link AuditAction} vocabulary. */
    @NotNull
    private AuditAction action;

    private String details;

    /** Owning organization. Optional. */
    private Integer organizationId;

    /** State AFTER the change. Optional. */
    private Map<String, Object> newPayload;

    /** The delta between old and new state. Optional. */
    private Map<String, Object> payloadDifference;

    /** State BEFORE the change. Optional. */
    private Map<String, Object> oldPayload;

    /** Free-form extra fields the emitting service wants to attach. Optional. */
    private Map<String, Object> metadata;

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

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

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

    public AuditAction getAction() { return action; }
    public void setAction(AuditAction action) { this.action = action; }

    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }

    public Integer getOrganizationId() { return organizationId; }
    public void setOrganizationId(Integer organizationId) { this.organizationId = organizationId; }

    public Map<String, Object> getNewPayload() { return newPayload; }
    public void setNewPayload(Map<String, Object> newPayload) { this.newPayload = newPayload; }

    public Map<String, Object> getPayloadDifference() { return payloadDifference; }
    public void setPayloadDifference(Map<String, Object> payloadDifference) { this.payloadDifference = payloadDifference; }

    public Map<String, Object> getOldPayload() { return oldPayload; }
    public void setOldPayload(Map<String, Object> oldPayload) { this.oldPayload = oldPayload; }

    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
