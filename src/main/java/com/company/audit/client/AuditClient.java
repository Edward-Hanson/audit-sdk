package com.company.audit.client;

import com.company.audit.config.AuditProperties;
import com.company.audit.exception.AuditValidationException;
import com.company.audit.model.AuditEvent;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The entry point teams use:
 *
 *   auditClient.send(
 *       AuditEventBuilder.builder()
 *           .userName("jane").userId(42L)
 *           .action("SALARY_CHANGED")
 *           .entityType("EMPLOYEE").entityId("99")
 *           .organizationId(1)
 *           .build());
 *
 * Responsibilities:
 *   1. Stamp SDK-owned defaults (sourceService, eventId, timestamp).
 *   2. Validate the event (Bean Validation) — clear error on failure.
 *   3. Serialize + produce to Kafka, keyed for per-entity ordering.
 *   4. Handle send failures per the fail-on-error policy without breaking the caller.
 */
public class AuditClient implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(AuditClient.class);

    /**
     * The single audit topic every service publishes to. Owned by the SDK and NOT
     * configurable — this guarantees all audit events land on one governed topic and
     * no consuming team can redirect them via config.
     */
    public static final String TOPIC = "audit-service";

    private final boolean enabled;
    private final KafkaTemplate<String, AuditEvent> kafkaTemplate;
    private final AuditProperties properties;
    private final Validator validator;

    /**
     * Producer factory owned by this client, or {@code null} when the caller
     * supplied a template whose lifecycle it manages itself. Not exposed as a
     * Spring bean (see {@code AuditAutoConfiguration}); this client is therefore
     * responsible for closing it on shutdown via {@link #destroy()}.
     */
    private final DefaultKafkaProducerFactory<String, AuditEvent> ownedProducerFactory;

    /**
     * Full constructor used by the auto-configuration: the client owns and closes
     * {@code producerFactory} on shutdown.
     */
    public AuditClient(KafkaTemplate<String, AuditEvent> kafkaTemplate,
                       DefaultKafkaProducerFactory<String, AuditEvent> producerFactory,
                       AuditProperties properties,
                       Validator validator) {
        this.enabled = true;
        this.kafkaTemplate = kafkaTemplate;
        this.ownedProducerFactory = producerFactory;
        this.properties = properties;
        this.validator = validator;
    }

    /**
     * Convenience constructor for callers that own the template lifecycle themselves
     * (e.g. tests, or an app supplying its own {@code KafkaTemplate}). No producer
     * factory is closed on shutdown.
     */
    public AuditClient(KafkaTemplate<String, AuditEvent> kafkaTemplate,
                       AuditProperties properties,
                       Validator validator) {
        this(kafkaTemplate, null, properties, validator);
    }

    /** No-op constructor used when {@code audit.enabled=false}. Touches no Kafka. */
    private AuditClient() {
        this.enabled = false;
        this.kafkaTemplate = null;
        this.ownedProducerFactory = null;
        this.properties = null;
        this.validator = null;
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

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            // A transaction is in progress: defer the actual publish until it COMMITS.
            // If the transaction rolls back, afterCommit never fires and the event is
            // never sent — so we never emit an audit record for work that was undone.
            publishAfterCommit(event);
        } else if (properties.isFailOnError()) {
            sendAndWait(event);
        } else {
            sendBestEffort(event);
        }
    }

    /**
     * Registers the publish to run after the surrounding transaction commits.
     *
     * <p>Note on {@code fail-on-error}: once the transaction has committed the business
     * data is already durable and cannot be rolled back by an audit failure, so the
     * deferred publish is always best-effort here — {@code fail-on-error=true}'s blocking
     * / rethrow behavior only applies when there is no active transaction.
     */
    private void publishAfterCommit(AuditEvent event) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sendBestEffort(event);
            }
        });
    }

    /**
     * Best-effort (default): never blocks and never breaks the caller. Failures —
     * whether thrown synchronously by send() (e.g. max.block.ms exceeded, serialization)
     * or reported asynchronously by the broker — are logged and swallowed.
     */
    private void sendBestEffort(AuditEvent event) {
        try {
            // Null key: per-entity ordering is not required, so we let Kafka's sticky
            // partitioner spread events evenly across the topic's partitions.
            kafkaTemplate.send(TOPIC, event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logFailure(event, ex);
                        } else if (log.isDebugEnabled()) {
                            log.debug("Audit event {} sent to {}", event.getEventId(), TOPIC);
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
        try {
            kafkaTemplate.send(TOPIC, event)
                    .get(properties.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (log.isDebugEnabled()) {
                log.debug("Audit event {} sent to {}", event.getEventId(), TOPIC);
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
        event.setSourceService(properties.getSourceService());
        if (!StringUtils.hasText(event.getEventId())) {
            event.setEventId(java.util.UUID.randomUUID().toString());
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
     * Closes the owned producer factory on context shutdown so buffered records are
     * flushed and the producer's threads/connections are released. No-op when the
     * template lifecycle is owned elsewhere.
     */
    @Override
    public void destroy() {
        if (ownedProducerFactory != null) {
            ownedProducerFactory.destroy();
        }
    }
}
