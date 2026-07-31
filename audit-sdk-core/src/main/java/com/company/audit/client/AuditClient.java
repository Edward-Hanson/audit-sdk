package com.company.audit.client;

import com.company.audit.config.AuditProperties;
import com.company.audit.core.TransactionHook;
import com.company.audit.exception.AuditValidationException;
import com.company.audit.model.AuditEvent;
import com.company.audit.util.Text;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The entry point teams use:
 *
 *   auditClient.send(
 *       AuditEventBuilder.builder()
 *           .userName("jane").userId(42L)
 *           .action(AuditAction.UPDATE)
 *           .entityType("EMPLOYEE").entityId("99")
 *           .organizationId(1)
 *           .build());
 *
 * Responsibilities:
 *   1. Stamp SDK-owned defaults (sourceService, clientId, eventId, timestamp).
 *   2. Validate the event (Bean Validation) — clear error on failure.
 *   3. Serialize to JSON and produce to Kafka (no key → sticky partitioning).
 *   4. Handle send failures per the fail-on-error policy without breaking the caller.
 *
 * <p>Framework-neutral: this class depends only on {@code kafka-clients}, Jackson and
 * Jakarta Validation. Transaction awareness is delegated to a {@link TransactionHook}
 * supplied by the starter, so the same compiled class runs under any Spring Boot generation.
 */
public class AuditClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditClient.class);

    /** The audit topic to publish to — supplied from {@code audit.kafka.topic} config. */
    private final String topic;

    /** Verified Entra client id (from {@code entra.client-id}). Stamped onto every event. */
    private final String clientId;

    private final boolean enabled;
    private final Producer<String, String> producer;
    private final ObjectMapper mapper;
    private final AuditProperties properties;
    private final Validator validator;
    private final TransactionHook txHook;

    /**
     * Full constructor used by the auto-configuration. The client owns {@code producer} and
     * closes it on shutdown via {@link #close()}.
     */
    public AuditClient(String topic,
                       String clientId,
                       Producer<String, String> producer,
                       ObjectMapper mapper,
                       AuditProperties properties,
                       Validator validator,
                       TransactionHook txHook) {
        this.enabled = true;
        this.topic = topic;
        this.clientId = clientId;
        this.producer = producer;
        this.mapper = mapper;
        this.properties = properties;
        this.validator = validator;
        this.txHook = txHook != null ? txHook : TransactionHook.none();
    }

    /** No-op constructor used when {@code audit.enabled=false}. Touches no Kafka. */
    private AuditClient() {
        this.enabled = false;
        this.topic = null;
        this.clientId = null;
        this.producer = null;
        this.mapper = null;
        this.properties = null;
        this.validator = null;
        this.txHook = TransactionHook.none();
    }

    /**
     * A disabled client: {@link #send} does nothing and no Kafka producer is created.
     * Used when {@code audit.enabled=false} so a service with no Kafka can keep its
     * {@code auditClient.send(...)} calls in place as clean no-ops.
     */
    public static AuditClient disabled() {
        return new AuditClient();
    }

    public void send(AuditEvent event) {
        if (!enabled) {
            if (log.isDebugEnabled()) {
                log.debug("Auditing disabled (audit.enabled=false); skipping event action={}",
                        event.getAction());
            }
            return;
        }

        // Stamp + validate synchronously so the caller gets immediate feedback
        // (AuditValidationException) regardless of any surrounding transaction.
        stampDefaults(event);
        validate(event);

        if (txHook.isActive()) {
            // A transaction is in progress: defer the actual publish until it COMMITS.
            // If the transaction rolls back, afterCommit never fires and the event is
            // never sent — so we never emit an audit record for work that was undone.
            txHook.afterCommit(() -> sendBestEffort(event));
        } else if (properties.isFailOnError()) {
            sendAndWait(event);
        } else {
            sendBestEffort(event);
        }
    }

    /**
     * Best-effort (default): never blocks and never breaks the caller. Failures —
     * whether thrown synchronously by send() (e.g. max.block.ms exceeded, serialization)
     * or reported asynchronously by the broker — are logged and swallowed.
     */
    private void sendBestEffort(AuditEvent event) {
        final String json;
        try {
            json = mapper.writeValueAsString(event);
        } catch (Exception ex) {
            logFailure(event, ex);
            return;
        }
        try {
            // Null key: per-entity ordering is not required, so we let Kafka's sticky
            // partitioner spread events evenly across the topic's partitions.
            producer.send(new ProducerRecord<>(topic, json), (metadata, ex) -> {
                if (ex != null) {
                    logFailure(event, ex);
                } else if (log.isDebugEnabled()) {
                    log.debug("Audit event {} sent to {}", event.getEventId(), topic);
                }
            });
        } catch (Exception ex) {
            // send() itself can throw before returning a future; a failed audit log
            // must still not break the business action.
            logFailure(event, ex);
        }
    }

    /**
     * fail-on-error=true: the caller has opted in to letting an audit failure break
     * the business action. Block up to {@code audit.send-timeout} for the broker ack
     * and rethrow synchronously so the exception actually reaches the caller.
     */
    private void sendAndWait(AuditEvent event) {
        final String json;
        try {
            json = mapper.writeValueAsString(event);
        } catch (Exception ex) {
            throw failure(event, ex);
        }
        try {
            producer.send(new ProducerRecord<>(topic, json))
                    .get(properties.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (log.isDebugEnabled()) {
                log.debug("Audit event {} sent to {}", event.getEventId(), topic);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failure(event, ex);
        } catch (ExecutionException ex) {
            throw failure(event, ex.getCause() != null ? ex.getCause() : ex);
        } catch (Exception ex) {
            // TimeoutException from get(), or a synchronous throw from send().
            throw failure(event, ex);
        }
    }

    private void logFailure(AuditEvent event, Throwable ex) {
        log.error("Failed to send audit event {} (action={}, source={})",
                event.getEventId(), event.getAction(), event.getSourceService(), ex);
    }

    private IllegalStateException failure(AuditEvent event, Throwable cause) {
        logFailure(event, cause);
        return new IllegalStateException("Failed to send audit event " + event.getEventId(), cause);
    }

    private void stampDefaults(AuditEvent event) {
        // sourceService (display name) and clientId always come from config, never the caller.
        event.setSourceService(properties.getDisplayName());
        event.setClientId(clientId);
        if (!Text.hasText(event.getEventId())) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(java.time.Instant.now());
        }
    }

    private void validate(AuditEvent event) {
        Set<ConstraintViolation<AuditEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new AuditValidationException("Invalid audit event: " + message);
        }
    }

    /**
     * Closes the owned producer on context shutdown so buffered records are flushed and the
     * producer's threads/connections are released. Wired as the bean {@code destroyMethod}.
     * No-op for a disabled client.
     */
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
