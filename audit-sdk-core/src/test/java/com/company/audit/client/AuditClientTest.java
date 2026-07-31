package com.company.audit.client;

import com.company.audit.config.AuditProperties;
import com.company.audit.core.TransactionHook;
import com.company.audit.exception.AuditValidationException;
import com.company.audit.model.AuditAction;
import com.company.audit.model.AuditEvent;
import com.company.audit.model.AuditEventBuilder;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the send/validate/stamp behavior of {@link AuditClient}, using a mocked
 * {@link Producer} so no broker is required. No Spring on the classpath — transaction
 * awareness is exercised through a manual {@link TransactionHook}.
 */
class AuditClientTest {

    private static final Validator VALIDATOR =
            Validation.buildDefaultValidatorFactory().getValidator();

    private static final String TOPIC = "audit_service_test";
    private static final String CLIENT_ID = "client-abc";

    @SuppressWarnings("unchecked")
    private final Producer<String, String> producer = mock(Producer.class);

    private AuditProperties properties;
    private ManualTxHook txHook;
    private AuditClient client;

    @BeforeEach
    void setUp() {
        properties = new AuditProperties();
        properties.setDisplayName("Payroll");
        txHook = new ManualTxHook();
        client = new AuditClient(TOPIC, CLIENT_ID, producer, AuditJson.mapper(), properties, VALIDATOR, txHook);
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

    @Test
    @SuppressWarnings("unchecked")
    void sendsValidEventToTheConfiguredTopicWithNoKey() {
        client.send(validEvent());

        // Best-effort path uses send(record, callback). Publishes to the configured topic
        // with no key, so Kafka's sticky partitioner spreads events evenly.
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture(), any(Callback.class));
        assertThat(captor.getValue().topic()).isEqualTo(TOPIC);
        assertThat(captor.getValue().key()).isNull();
        assertThat(captor.getValue().value()).contains("\"userName\":\"jane.admin\"");
    }

    @Test
    void stampsSdkOwnedDefaults() {
        AuditEvent event = validEvent();
        client.send(event);

        assertThat(event.getSourceService()).isEqualTo("Payroll"); // display-name from config, not caller
        assertThat(event.getClientId()).isEqualTo(CLIENT_ID);      // entra.client-id from config
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getTimestamp()).isNotNull();
    }

    @Test
    void sourceServiceAndClientIdAlwaysComeFromConfig() {
        AuditEvent event = validEvent();
        event.setSourceService("spoofed"); // caller attempts are overwritten
        event.setClientId("spoofed-client");
        client.send(event);

        assertThat(event.getSourceService()).isEqualTo("Payroll");
        assertThat(event.getClientId()).isEqualTo(CLIENT_ID);
    }

    @Test
    void doesNotOverwriteCallerSuppliedTimestamp() {
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
                .build(); // missing required userName, userId, entityId, entityType

        assertThatThrownBy(() -> client.send(event))
                .isInstanceOf(AuditValidationException.class)
                .hasMessageContaining("userName")
                .hasMessageContaining("userId")
                .hasMessageContaining("entityId")
                .hasMessageContaining("entityType");

        verify(producer, never()).send(any());
        verify(producer, never()).send(any(), any());
    }

    @Test
    void rejectsEventWithNoAction() {
        AuditEvent event = AuditEventBuilder.builder()
                .userName("jane.admin").userId(42L)
                .entityType("EMPLOYEE").entityId("99")
                .build(); // no action

        assertThatThrownBy(() -> client.send(event))
                .isInstanceOf(AuditValidationException.class)
                .hasMessageContaining("action");

        verify(producer, never()).send(any());
        verify(producer, never()).send(any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendFailureDoesNotBreakCallerWhenFailOnErrorFalse() {
        properties.setFailOnError(false);
        // Broker reports the failure asynchronously via the callback.
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            Callback cb = inv.getArgument(1);
            cb.onCompletion(null, new RuntimeException("broker down"));
            return CompletableFuture.completedFuture(null);
        });

        // The whole point of the SDK: a failed audit send must not throw into the
        // caller's business action.
        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousSendErrorDoesNotBreakCallerWhenFailOnErrorFalse() {
        properties.setFailOnError(false);
        // send() can throw before returning a future (e.g. max.block.ms exceeded).
        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("metadata timeout"));

        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendFailurePropagatesWhenFailOnErrorTrue() {
        properties.setFailOnError(true);
        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        // Caller opted in: the failure must reach them, with the root cause attached.
        assertThatThrownBy(() -> client.send(validEvent()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to send audit event")
                .hasRootCauseMessage("broker down");
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendSucceedsSynchronouslyWhenFailOnErrorTrue() {
        properties.setFailOnError(true);
        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThatCode(() -> client.send(validEvent())).doesNotThrowAnyException();
        verify(producer).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void inTransactionPublishIsDeferredUntilCommit() {
        txHook.active = true;

        client.send(validEvent());

        // Nothing published yet — we're mid-transaction.
        verify(producer, never()).send(any(), any());

        // Simulate the transaction committing.
        txHook.commit();

        // Now — and only now — the event is published.
        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    void inTransactionNothingIsPublishedOnRollback() {
        txHook.active = true;

        client.send(validEvent());
        // Transaction rolls back: we simply never invoke the registered after-commit action.

        verify(producer, never()).send(any());
        verify(producer, never()).send(any(), any());
    }

    @Test
    void disabledClientPublishesNothing() {
        AuditClient disabled = AuditClient.disabled();

        // No exception even for an "invalid" event, and nothing is sent.
        assertThatCode(() -> disabled.send(new AuditEvent())).doesNotThrowAnyException();
        verify(producer, never()).send(any());
        verify(producer, never()).send(any(), any());
    }

    /** A manual {@link TransactionHook} that captures the after-commit action for the test to fire. */
    private static final class ManualTxHook implements TransactionHook {
        boolean active;
        private final List<Runnable> pending = new ArrayList<>();

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void afterCommit(Runnable action) {
            pending.add(action);
        }

        void commit() {
            pending.forEach(Runnable::run);
        }
    }
}
