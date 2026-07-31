package com.company.audit.config;

import com.company.audit.client.AuditClient;
import com.company.audit.client.AuditJson;
import com.company.audit.client.AuditProducers;
import com.company.audit.core.TransactionHook;
import com.company.audit.registration.AuditServiceRegistrar;
import com.company.audit.registration.EntraTokenClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Spring Boot auto-configuration: wires the framework-neutral {@code audit-sdk-core} into a
 * Spring application. Pulled in automatically when the SDK is on the classpath.
 *
 * <p>This source is <b>shared, compiled once per Boot generation</b>: the
 * {@code audit-sdk-spring-boot-3} and {@code audit-sdk-spring-boot-4} modules both compile it
 * (via build-helper) against their own Spring version. Every Spring type it references —
 * {@code @AutoConfiguration}, the {@code @ConditionalOn*} conditions, {@code @ConfigurationProperties},
 * {@code ApplicationRunner} — lives at the identical package coordinates in Boot 3 and Boot 4,
 * so the same source is valid for both.
 *
 * <p>The audit producer connects to its OWN broker ({@code audit.kafka.servers}) and publishes
 * to its OWN topic ({@code audit.kafka.topic}), deliberately decoupled from the host app's
 * {@code spring.kafka.*}. These values MUST be supplied when auditing is enabled; the app
 * fails to start otherwise.
 *
 * <p>Bean isolation: the SDK creates its Kafka producer as a plain object owned by the single
 * {@link AuditClient} bean (closed on shutdown via the {@code close} destroy-method). It uses
 * the raw {@code kafka-clients} producer — not {@code spring-kafka} — so it contributes no
 * {@code ProducerFactory}/{@code KafkaTemplate} beans and cannot suppress the host app's own
 * Kafka autoconfiguration.
 */
@AutoConfiguration
@EnableConfigurationProperties
public class AuditAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "audit")
    public AuditProperties auditProperties() {
        return new AuditProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "entra")
    public EntraProperties entraProperties() {
        return new EntraProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public Validator auditValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Bean
    @ConditionalOnMissingBean
    public TransactionHook auditTransactionHook() {
        return new SpringTransactionHook();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "audit", name = "enabled", havingValue = "true", matchIfMissing = true)
    public AuditClient auditClient(AuditProperties auditProperties,
                                   EntraProperties entraProperties,
                                   Validator auditValidator,
                                   TransactionHook transactionHook) {
        // Fail fast on required configuration — the app must not start without it.
        String servers = auditProperties.getKafka().getServers();
        String topic = auditProperties.getKafka().getTopic();
        String entraClientId = entraProperties.getClientId();
        require(auditProperties.getDisplayName(), "audit.display-name", "Payroll");
        require(entraClientId, "entra.client-id", "<your-entra-client-id>");
        require(servers, "audit.kafka.servers", "broker1:9092,broker2:9092");
        require(topic, "audit.kafka.topic", "audit_service_dev");

        Producer<String, String> producer =
                AuditProducers.create(servers, entraClientId, auditProperties.getKafka().getProperties());
        return new AuditClient(topic, entraClientId, producer, AuditJson.mapper(),
                auditProperties, auditValidator, transactionHook);
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
     * Required config is validated when this bean is created (fail-fast at context refresh);
     * the actual token + {@code POST /register} call runs in the {@link ApplicationRunner} so
     * it fails app startup — but is not triggered by Spring's test context runner.
     */
    @Bean
    @ConditionalOnProperty(prefix = "audit", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ApplicationRunner auditRegistrationRunner(AuditProperties auditProperties,
                                                     EntraProperties entraProperties) {
        require(entraProperties.getClientId(), "entra.client-id", "<your-entra-client-id>");
        require(entraProperties.getClientSecret(), "entra.client-secret", "<your-entra-client-secret>");
        require(entraProperties.getTenantId(), "entra.tenant-id", "<your-entra-tenant-id>");
        require(auditProperties.getUrl(), "audit.url", "https://audit.internal");
        require(auditProperties.getScope(), "audit.scope", "api://<audit-app-id>/.default");

        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        EntraTokenClient tokenClient =
                new EntraTokenClient(http, new ObjectMapper(), entraProperties, auditProperties.getScope());
        AuditServiceRegistrar registrar = new AuditServiceRegistrar(
                http, tokenClient, auditProperties.getUrl(), auditProperties.getDisplayName());
        return args -> registrar.register();
    }

    private static void require(String value, String property, String example) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalStateException(
                    "'" + property + "' is required but is not set. Add it to your "
                            + "application.yml/application.properties, e.g. '" + property + ": " + example + "'.");
        }
    }
}
