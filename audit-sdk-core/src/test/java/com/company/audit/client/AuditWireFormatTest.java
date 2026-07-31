package com.company.audit.client;

import com.company.audit.config.AuditProperties;
import com.company.audit.core.TransactionHook;
import com.company.audit.model.AuditAction;
import com.company.audit.model.AuditEventBuilder;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the Kafka wire format the audit consumer depends on. There is no schema registry, so
 * this JSON shape IS the contract: ISO-8601 timestamps (not epoch millis), enums by name,
 * plain JSON value with no Spring type header, and no key. Captures the exact bytes handed to
 * the Kafka producer.
 */
class AuditWireFormatTest {

    private static final Validator VALIDATOR =
            Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    @SuppressWarnings("unchecked")
    void publishesCleanJsonWithIso8601TimestampAndNoKey() {
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        AuditProperties props = new AuditProperties();
        props.setDisplayName("Payroll");
        props.setFailOnError(true); // use the blocking path so the record is produced synchronously
        AuditClient client = new AuditClient(
                "audit_service_it", "payroll-client-id", producer, AuditJson.mapper(),
                props, VALIDATOR, TransactionHook.none());

        Instant ts = Instant.parse("2026-07-13T10:15:30Z");
        client.send(AuditEventBuilder.builder()
                .userName("jane.admin")
                .userId(42L)
                .action(AuditAction.UPDATE)
                .entityType("EMPLOYEE")
                .entityId("99")
                .entityName("Jane")
                .organizationId(1)
                .details("Adjusted annual salary")
                .oldPayload(Map.of("salary", 100))
                .newPayload(Map.of("salary", 120))
                .payloadDifference(Map.of("salary", 20))
                .timestamp(ts)
                .build());

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        // Null key → sticky partitioning; plain JSON value (no Spring __TypeId__ header exists).
        assertThat(record.key()).isNull();

        String json = record.value();
        assertThat(json)
                .contains("\"sourceService\":\"Payroll\"")
                .contains("\"clientId\":\"payroll-client-id\"")
                .contains("\"userName\":\"jane.admin\"")
                .contains("\"userId\":42")
                // Enum serialized by name.
                .contains("\"action\":\"UPDATE\"")
                .contains("\"entityType\":\"EMPLOYEE\"")
                .contains("\"entityId\":\"99\"")
                .contains("\"organizationId\":1")
                .contains("\"eventId\":")
                .contains("\"oldPayload\":")
                .contains("\"newPayload\":")
                .contains("\"payloadDifference\":")
                // Instant serialized as ISO-8601, NOT epoch millis.
                .contains("\"timestamp\":\"2026-07-13T10:15:30Z\"")
                .doesNotContain("__TypeId__");
    }
}
