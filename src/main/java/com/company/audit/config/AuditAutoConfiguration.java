package com.company.audit.config;

import com.company.audit.client.AuditClient;
import com.company.audit.model.AuditEvent;
import com.company.audit.registration.AuditServiceRegistrar;
import com.company.audit.registration.EntraTokenClient;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.StringUtils;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration: pulled in automatically when the SDK is on the classpath.
 *
 * <p>The audit producer connects to its OWN broker ({@code audit.kafka.servers}) and
 * publishes to its OWN topic ({@code audit.kafka.topic}) — deliberately decoupled from
 * the host app's {@code spring.kafka.*}. Nothing about the destination is baked into the
 * SDK, so each environment (dev/stage/prod) points at its own cluster and topic and can't
 * cross-publish. These values MUST be supplied when auditing is enabled; the app fails to
 * start otherwise.
 *
 * <p>IMPORTANT — bean isolation: this SDK deliberately does <b>not</b> expose the audit
 * {@code ProducerFactory} or {@code KafkaTemplate} as Spring beans. Spring Boot's
 * {@code KafkaAutoConfiguration} creates its default {@code producerFactory}/{@code
 * kafkaTemplate} under {@code @ConditionalOnMissingBean} on the <i>raw</i> types (generics
 * are erased for the condition), so registering our own beans of those types would silently
 * suppress the host app's defaults. Instead the audit producer/template are constructed as
 * private objects owned by the single {@link AuditClient} bean, which closes the producer
 * factory on shutdown. The host app's own Kafka autoconfiguration is left untouched.
 */
@AutoConfiguration
@EnableConfigurationProperties({AuditProperties.class, EntraProperties.class})
public class AuditAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Validator auditValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "audit", name = "enabled", havingValue = "true", matchIfMissing = true)
    public AuditClient auditClient(AuditProperties auditProperties,
                                   EntraProperties entraProperties,
                                   Validator auditValidator) {
        // Fail fast on required configuration — the app must not start without it.
        String servers = auditProperties.getKafka().getServers();
        String topic = auditProperties.getKafka().getTopic();
        String entraClientId = entraProperties.getClientId();
        require(auditProperties.getDisplayName(), "audit.display-name", "Payroll");
        require(entraClientId, "entra.client-id", "<your-entra-client-id>");
        require(servers, "audit.kafka.servers", "broker1:9092,broker2:9092");
        require(topic, "audit.kafka.topic", "audit_service_dev");

        // Built as plain objects, NOT Spring beans, so we don't trip
        // KafkaAutoConfiguration's @ConditionalOnMissingBean(ProducerFactory/KafkaTemplate).
        DefaultKafkaProducerFactory<String, AuditEvent> producerFactory =
                auditProducerFactory(servers, entraClientId, auditProperties.getKafka().getProperties());
        KafkaTemplate<String, AuditEvent> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // AuditClient owns the producer factory's lifecycle: as a Spring bean it will
        // have destroy() invoked on context shutdown, which closes the producer cleanly.
        return new AuditClient(topic, entraClientId, kafkaTemplate, producerFactory, auditProperties, auditValidator);
    }

    /**
     * When {@code audit.enabled=false}, expose a no-op client so apps keep their
     * {@code auditClient.send(...)} calls in place while nothing is published and no
     * Kafka producer is created (no broker required).
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "audit", name = "enabled", havingValue = "false")
    public AuditClient disabledAuditClient() {
        return AuditClient.disabled();
    }

    /**
     * Registers this app with the audit service once at startup (before any events flow).
     * Required config is validated here (fail-fast at context refresh); the actual token +
     * {@code POST /register} call runs as an {@link AuditServiceRegistrar} ApplicationRunner
     * so it fails app startup — but is not triggered by Spring's test context runner.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "audit", name = "enabled", havingValue = "true", matchIfMissing = true)
    public AuditServiceRegistrar auditServiceRegistrar(AuditProperties auditProperties,
                                                       EntraProperties entraProperties) {
        require(entraProperties.getClientId(), "entra.client-id", "<your-entra-client-id>");
        require(entraProperties.getClientSecret(), "entra.client-secret", "<your-entra-client-secret>");
        require(entraProperties.getTenantId(), "entra.tenant-id", "<your-entra-tenant-id>");
        require(auditProperties.getUrl(), "audit.url", "https://audit.internal");
        require(auditProperties.getScope(), "audit.scope", "api://<audit-app-id>/.default");

        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        EntraTokenClient tokenClient =
                new EntraTokenClient(http, new ObjectMapper(), entraProperties, auditProperties.getScope());
        return new AuditServiceRegistrar(
                http, tokenClient, auditProperties.getUrl(), auditProperties.getDisplayName());
    }

    private static void require(String value, String property, String example) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalStateException(
                    "'" + property + "' is required but is not set. Add it to your "
                            + "application.yml/application.properties, e.g. '" + property + ": " + example + "'.");
        }
    }

    /**
     * Producer factory tuned for audit reliability, targeting the dedicated audit broker:
     *  - idempotent producer (no duplicates from producer-side retries)
     *  - acks=all (wait for replicas)
     *  - retries + delivery timeout so transient broker issues self-heal
     *  - bounded max.block.ms so a broker outage can't stall the caller
     *  - JSON value serializer (no schema registry)
     *
     * Not a {@code @Bean} — see the class-level note on bean isolation.
     */
    private DefaultKafkaProducerFactory<String, AuditEvent> auditProducerFactory(
            String servers, String clientId, Map<String, String> passthrough) {

        Map<String, Object> props = new HashMap<>();
        // Optional per-environment extras first (e.g. security.protocol, sasl.*), so the
        // SDK-owned settings below always win over anything the passthrough might set.
        if (passthrough != null) {
            props.putAll(passthrough);
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000);

        // JSON value serializer backed by a JavaTime-aware ObjectMapper so Instant
        // serializes as ISO-8601. addTypeInfo=false keeps the payload clean (no
        // __TypeId__ header) — the consumer knows it's an AuditEvent.
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<AuditEvent> valueSerializer = new JsonSerializer<>(mapper);
        valueSerializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }
}
