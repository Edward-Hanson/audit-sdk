package com.company.audit.config;

import com.company.audit.client.AuditClient;
import com.company.audit.core.TransactionHook;
import com.company.audit.model.AuditEvent;

import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Wiring tests for the starter, run against whichever Spring Boot generation compiled this
 * source (Boot 3 in {@code audit-sdk-spring-boot-3}, Boot 4 in {@code audit-sdk-spring-boot-4}).
 *
 * <p>Note on bean isolation: because the SDK now uses the raw {@code kafka-clients} producer
 * rather than {@code spring-kafka}, it contributes no {@code ProducerFactory}/{@code KafkaTemplate}
 * beans at all — so it structurally cannot suppress the host app's Kafka autoconfiguration.
 * (The old "does not suppress Boot's Kafka beans" test is therefore obsolete.)
 */
class AuditAutoConfigurationTest {

    /** A complete, valid set of required properties (keyed by property name). */
    private static Map<String, String> validProps() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("audit.display-name", "Payroll");
        p.put("entra.client-id", "payroll-client-id");
        p.put("entra.client-secret", "shhh");
        p.put("entra.tenant-id", "tenant-123");
        p.put("audit.url", "https://audit.internal");
        p.put("audit.scope", "api://audit/.default");
        p.put("audit.kafka.servers", "localhost:9092");
        p.put("audit.kafka.topic", "audit_service_test");
        return p;
    }

    private static ApplicationContextRunner runnerWith(Map<String, String> props) {
        String[] values = props.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .toArray(String[]::new);
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AuditAutoConfiguration.class))
                .withPropertyValues(values);
    }

    private final ApplicationContextRunner runner = runnerWith(validProps());

    @Test
    void wiresUpAuditClientValidatorAndProperties() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(AuditClient.class);
            assertThat(context).hasSingleBean(Validator.class);
            assertThat(context).hasSingleBean(TransactionHook.class);
            assertThat(context).hasSingleBean(AuditProperties.class);
            assertThat(context.getBean(AuditProperties.class).getDisplayName()).isEqualTo("Payroll");
            // Nested + scope properties bind correctly through the shared POJO.
            assertThat(context.getBean(AuditProperties.class).getKafka().getTopic())
                    .isEqualTo("audit_service_test");
            assertThat(context.getBean(EntraProperties.class).getClientId()).isEqualTo("payroll-client-id");
        });
    }

    @Test
    void failsFastWhenAnyRequiredPropertyIsMissing() {
        // Each required property, omitted in turn, must fail startup with a message naming it.
        for (String property : validProps().keySet()) {
            Map<String, String> props = validProps();
            props.remove(property);
            runnerWith(props).run(context -> {
                assertThat(context).as("missing " + property).hasFailed();
                assertThat(context.getStartupFailure())
                        .hasRootCauseInstanceOf(IllegalStateException.class);
                assertThat(context.getStartupFailure().getMessage()).contains(property);
            });
        }
    }

    @Test
    void auditEnabledFalseGivesANoOpClientAndNeedsNoConfig() {
        // No display-name, no bootstrap-servers — a disabled service should still wire up
        // cleanly and send() must be a harmless no-op.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AuditAutoConfiguration.class))
                .withPropertyValues("audit.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(AuditClient.class);
                    assertThatCode(() -> context.getBean(AuditClient.class).send(new AuditEvent()))
                            .doesNotThrowAnyException();
                });
    }

    @Test
    void auditClientIsOverridable() {
        runner.withUserConfiguration(CustomClientConfig.class).run(context -> {
            assertThat(context).hasSingleBean(AuditClient.class);
            assertThat(context.getBean(AuditClient.class)).isSameAs(CustomClientConfig.CUSTOM);
        });
    }

    @Configuration
    static class CustomClientConfig {
        static final AuditClient CUSTOM = AuditClient.disabled();

        @Bean
        AuditClient auditClient() {
            return CUSTOM;
        }
    }
}
