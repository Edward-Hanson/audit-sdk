package com.company.audit.config;

import com.company.audit.client.AuditClient;
import com.company.audit.model.AuditEvent;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration: pulled in automatically when the SDK is on the classpath.
 * Teams only provide spring.kafka.bootstrap-servers and audit.* properties.
 *
 * <p>IMPORTANT — bean isolation: this SDK deliberately does <b>not</b> expose the
 * audit {@code ProducerFactory} or {@code KafkaTemplate} as Spring beans. Spring
 * Boot's {@code KafkaAutoConfiguration} creates its default {@code producerFactory}
 * and {@code kafkaTemplate} under {@code @ConditionalOnMissingBean} on the <i>raw</i>
 * types {@code ProducerFactory}/{@code KafkaTemplate} (generics are erased for the
 * condition). If the SDK registered its own beans of those types, it would silently
 * suppress the host app's default Kafka beans just by being on the classpath. Instead
 * the audit producer/template are constructed as private objects owned by the single
 * {@link AuditClient} bean, which closes the producer factory on shutdown. The host
 * app's own Kafka autoconfiguration is left completely untouched.
 *
 * <p>The one public bean, {@link AuditClient}, is {@code @ConditionalOnMissingBean} so
 * an app can still supply its own.
 */
@AutoConfiguration
@EnableConfigurationProperties(AuditProperties.class)
public class AuditAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Validator auditValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditClient auditClient(KafkaProperties kafkaProperties,
                                   AuditProperties auditProperties,
                                   Validator auditValidator) {
        // Built as plain objects, NOT Spring beans, so we don't trip
        // KafkaAutoConfiguration's @ConditionalOnMissingBean(ProducerFactory/KafkaTemplate).
        DefaultKafkaProducerFactory<String, AuditEvent> producerFactory =
                auditProducerFactory(kafkaProperties);
        KafkaTemplate<String, AuditEvent> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // AuditClient owns the producer factory's lifecycle: as a Spring bean it will
        // have destroy() invoked on context shutdown, which closes the producer cleanly.
        return new AuditClient(kafkaTemplate, producerFactory, auditProperties, auditValidator);
    }

    /**
     * Producer factory tuned for audit reliability:
     *  - idempotent producer (no duplicates from producer-side retries)
     *  - acks=all (wait for replicas)
     *  - retries + delivery timeout so transient broker issues self-heal
     *  - JSON value serializer (no schema registry)
     *
     * Not a {@code @Bean} — see the class-level note on bean isolation.
     */
    private DefaultKafkaProducerFactory<String, AuditEvent> auditProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Bound how long a send() may block the CALLER'S thread waiting for cluster
        // metadata or buffer space. Without this, a broker outage makes the first
        // send() block up to the default 60s. 2s keeps the caller responsive; the
        // remaining reliability (retries up to delivery.timeout.ms) happens off the
        // caller's thread once the record is buffered.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000);

        // JSON value serializer backed by a JavaTime-aware ObjectMapper so
        // Instant serializes as ISO-8601. addTypeInfo=false keeps the payload
        // clean (no __TypeId__ header) — the consumer knows it's an AuditEvent.
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<AuditEvent> valueSerializer = new JsonSerializer<>(mapper);
        valueSerializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }
}
