package com.company.audit.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the audit Kafka producer, tuned for audit reliability and targeting the dedicated
 * audit broker:
 * <ul>
 *   <li>idempotent producer (no duplicates from producer-side retries)</li>
 *   <li>acks=all (wait for replicas)</li>
 *   <li>retries + delivery timeout so transient broker issues self-heal</li>
 *   <li>bounded max.block.ms so a broker outage can't stall the caller</li>
 * </ul>
 *
 * <p>The event is serialized to a JSON string by {@link AuditClient} before it reaches the
 * producer, so both key and value serializers are {@link StringSerializer} — the SDK depends
 * only on the long-stable {@code kafka-clients} producer API, not on {@code spring-kafka}.
 */
public final class AuditProducers {

    private AuditProducers() {
    }

    public static Producer<String, String> create(String servers, String clientId,
                                                   Map<String, String> passthrough) {
        Map<String, Object> props = new HashMap<>();
        // Optional per-environment extras first (e.g. security.protocol, sasl.*), so the
        // SDK-owned settings below always win over anything the passthrough might set.
        if (passthrough != null) {
            props.putAll(passthrough);
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000);
        return new KafkaProducer<>(props);
    }
}
