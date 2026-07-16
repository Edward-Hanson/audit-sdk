package com.company.audit.client;

import com.company.audit.config.AuditProperties;
import com.company.audit.exception.AuditValidationException;
import com.company.audit.model.AuditAction;
import com.company.audit.model.AuditEvent;
import com.company.audit.model.AuditEventBuilder;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the send/validate/stamp behavior of {@link AuditClient}, using a
 * mocked {@link KafkaTemplate} so no broker is required.
 */
class AuditClientTest {

    private static final Validator VALIDATOR =
            Validation.buildDefaultValidatorFactory().getValidator();

    private static final String TOPIC = "audit_service_test";
    private static final String CLIENT_ID = "client-abc";

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, AuditEvent> kafkaTemplate = mock(KafkaTemplate.class);

    private AuditProperties properties;
    private AuditClient client;

    @BeforeEach
    void setUp() {
        properties = new AuditProperties();
        properties.setDisplayName("Payroll");
        client = new AuditClient(TOPIC, CLIENT_ID, kafkaTemplate, properties, VALIDATOR);
    }

    private AuditEvent validEvent() {
        return AuditEventBuilder.builder()
                .userName("jane.admin")
                .userId(42L)
                .action(AuditAction.UPDATE)
                .entityType("EMPLOYEE")
                .entityId("99")
                .organizationId(1)
                .build();
    }

    private void templateSucceeds() {
        when(kafkaTemplate.send(anyString(), any(AuditEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    @Test
    void sendsValidEventToTheConfiguredTopicWithNoKey() {
        templateSucceeds();

        AuditEvent event = validEvent();
        client.send(event);

        // Publishes to the configured topic; no key is passed, so Kafka's sticky
        // partitioner spreads events evenly (ordering is not required).
        verify(kafkaTemplate).send(eq(TOPIC), eq(event));
    }

    @Test
    void stampsSdkOwnedDefaults() {
        templateSucceeds();

        AuditEvent event = validEvent();
        client.send(event);

        assertThat(event.getSourceService()).isEqualTo("Payroll"); // display-name from config, not caller
        assertThat(event.getClientId()).isEqualTo(CLIENT_ID);      // entra.client-id from config
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getTimestamp()).isNotNull();
    }

    @Test
    void sourceServiceAndClientIdAlwaysComeFromConfig() {
        templateSucceeds();

        AuditEvent event = validEvent();
        event.setSourceService("spoofed"); // caller attempts are overwritten
        event.setClientId("spoofed-client");
        client.send(event);

        assertThat(event.getSourceService()).isEqualTo("Payroll");
        assertThat(event.getClientId()).isEqualTo(CLIENT_ID);
    }

    @Test
    void doesNotOverwriteCallerSuppliedTimestamp() {
        templateSucceeds();

        Instant explicit = Instant.parse("2020-01-01T00:00:00Z");
        AuditEvent event = validEvent();
        event.setTimestamp(explicit);
        client.send(event);

        assertThat(event.getTimestamp()).isEqualTo(explicit);
    }

    @Test
    void rejectsInvalidEventWithFieldNamesAndDoesNotSend() {
        AuditEvent event = AuditEventBuilder.builder()
                .action(AuditAction.UPDATE)
                .build(); // missing required userName, userId, entityId, entityType (organizationId is optional)

        assertThatThrownBy(() -> client.send(event))
                .isInstanceOf(AuditValidationException.class)
                .hasMessageContaining("userName")
                .hasMessageContaining("userId")
                .hasMessageContaining("entityId")
                .hasMessageContaining("entityType");

        verify(kafkaTemplate, never()).send(anyString(), any(AuditEvent.class));
    }

    @Test
    void rejectsEventWithNoAction() {
        // The action vocabulary is enforced at compile time by the AuditAction enum;
        // presence is enforced here at runtime.
        AuditEvent event = AuditEventBuilder.builder()
                .userName("jane.admin").userId(42L)
                .entityType("EMPLOYEE").entityId("99")
                .build(); // no action

        assertThatThrownBy(() -> client.send(event))
                .isInstanceOf(AuditValidationException.class)
                .hasMessageContaining("action");

        verify(kafkaTemplate, never()).send(anyString(), any(AuditEvent.class));
    }

    @Test
    void sendFailureDoesNotBreakCallerWhenFailOnErrorFalse() {
        properties.setFailOnError(false);
        when(kafkaTemplate.send(anyString(), any(AuditEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        // The whole point of the SDK: a failed audit send must not throw into the
        // caller's business action.
        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
    }

    @Test
    void synchronousSendErrorDoesNotBreakCallerWhenFailOnErrorFalse() {
        properties.setFailOnError(false);
        // send() can throw before returning a future (e.g. max.block.ms exceeded).
        when(kafkaTemplate.send(anyString(), any(AuditEvent.class)))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("metadata timeout"));

        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
    }

    @Test
    void sendFailurePropagatesWhenFailOnErrorTrue() {
        properties.setFailOnError(true);
        when(kafkaTemplate.send(anyString(), any(AuditEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        // Caller opted in: the failure must reach them, with the root cause attached.
        assertThatThrownBy(() -> client.send(validEvent()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to send audit event")
                .hasRootCauseMessage("broker down");
    }

    @Test
    void sendSucceedsSynchronouslyWhenFailOnErrorTrue() {
        properties.setFailOnError(true);
        templateSucceeds();

        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
        verify(kafkaTemplate).send(eq(TOPIC), any(AuditEvent.class));
    }

    @Test
    void inTransactionPublishIsDeferredUntilCommit() {
        templateSucceeds();
        TransactionSynchronizationManager.initSynchronization();
        try {
            client.send(validEvent());

            // Nothing published yet — we're mid-transaction.
            verify(kafkaTemplate, never()).send(anyString(), any(AuditEvent.class));

            // Simulate the transaction committing.
            for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
                s.afterCommit();
            }

            // Now — and only now — the event is published.
            verify(kafkaTemplate).send(eq(TOPIC), any(AuditEvent.class));
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void disabledClientPublishesNothing() {
        AuditClient disabled = AuditClient.disabled();

        // No exception even for an "invalid" event, and nothing is sent.
        assertThatCode(() -> disabled.send(new AuditEvent())).doesNotThrowAnyException();
        verify(kafkaTemplate, never()).send(anyString(), any(AuditEvent.class));
    }

    @Test
    void inTransactionNothingIsPublishedOnRollback() {
        templateSucceeds();
        TransactionSynchronizationManager.initSynchronization();
        try {
            client.send(validEvent());

            // Transaction rolls back: afterCommit is never invoked (we just discard
            // the registered synchronizations, as the tx manager does on rollback).
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }

        // No audit event was ever sent for the rolled-back work.
        verify(kafkaTemplate, never()).send(anyString(), any(AuditEvent.class));
    }
}
