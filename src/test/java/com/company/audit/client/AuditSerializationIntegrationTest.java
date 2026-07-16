package com.company.audit.client;

import com.company.audit.config.AuditAutoConfiguration;
import com.company.audit.model.AuditAction;
import com.company.audit.model.AuditEventBuilder;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end serialization test against a real (embedded) Kafka broker. Proves the
 * SDK's actual producer path — JavaTime-aware JSON serializer, {@code addTypeInfo=false},
 * null key — produces the exact wire format the audit consumer expects.
 */
@EmbeddedKafka(partitions = 4, topics = AuditSerializationIntegrationTest.TOPIC)
class AuditSerializationIntegrationTest {

    static final String TOPIC = "audit_service_it";

    @Test
    void publishesCleanJsonWithIso8601TimestampAndNoKey() {
        EmbeddedKafkaBroker broker = EmbeddedKafkaCondition.getBroker();
        String bootstrap = broker.getBrokersAsString();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        KafkaAutoConfiguration.class, AuditAutoConfiguration.class))
                .withPropertyValues(
                        "audit.display-name=Payroll",
                        "entra.client-id=payroll-client-id",
                        // Registration config (validated at refresh; the ApplicationRunner
                        // that makes the real HTTP call is not triggered by the context runner).
                        "entra.client-secret=shhh",
                        "entra.tenant-id=tenant-123",
                        "audit.url=https://audit.internal",
                        // Dedicated audit broker + topic (embedded broker for the test).
                        "audit.kafka.servers=" + bootstrap,
                        "audit.kafka.topic=" + TOPIC,
                        // Block until the broker acks so the record is on the topic
                        // before we consume — makes the assertion deterministic.
                        "audit.fail-on-error=true",
                        "audit.send-timeout=15s")
                .run(context -> {
                    AuditClient client = context.getBean(AuditClient.class);

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

                    ConsumerRecord<String, String> record = consumeOne(broker);

                    // Null key → sticky partitioning.
                    assertThat(record.key()).isNull();

                    // No Spring type header — the payload is plain JSON.
                    assertThat(record.headers().lastHeader("__TypeId__")).isNull();

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
                            // Renamed payload fields.
                            .contains("\"oldPayload\":")
                            .contains("\"newPayload\":")
                            .contains("\"payloadDifference\":")
                            // Instant serialized as ISO-8601, NOT epoch millis.
                            .contains("\"timestamp\":\"2026-07-13T10:15:30Z\"")
                            .doesNotContain("__TypeId__");
                });
    }

    private ConsumerRecord<String, String> consumeOne(EmbeddedKafkaBroker broker) {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("audit-serialization-test", "true", broker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            consumer.subscribe(java.util.List.of(TOPIC));
            return KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(15));
        }
    }
}
