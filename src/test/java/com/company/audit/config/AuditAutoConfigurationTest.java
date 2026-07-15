package com.company.audit.config;

import com.company.audit.client.AuditClient;
import com.company.audit.model.AuditEvent;

import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

/**
 * Wiring + bean-isolation tests for the starter.
 *
 * The headline guarantee this file protects: dropping the SDK on the classpath must
 * NOT suppress the host app's own Kafka beans. Spring Boot's KafkaAutoConfiguration
 * creates its default producerFactory/kafkaTemplate under
 * {@code @ConditionalOnMissingBean(ProducerFactory.class / KafkaTemplate.class)} — a
 * raw-type condition. If the SDK registered its own beans of those types, Boot's
 * defaults would silently disappear. So we assert the SDK adds exactly zero beans of
 * those types.
 */
class AuditAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    KafkaAutoConfiguration.class, AuditAutoConfiguration.class))
            .withPropertyValues(
                    "audit.source-service=payroll",
                    "entra.client-id=payroll-client-id",
                    "spring.kafka.bootstrap-servers=localhost:9092");

    @Test
    void wiresUpAuditClientAndValidator() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(AuditClient.class);
            assertThat(context).hasSingleBean(Validator.class);
            assertThat(context).hasSingleBean(AuditProperties.class);
            assertThat(context.getBean(AuditProperties.class).getSourceService())
                    .isEqualTo("payroll");
        });
    }

    @Test
    void failsFastWhenClientIdMissing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        KafkaAutoConfiguration.class, AuditAutoConfiguration.class))
                .withPropertyValues(
                        "audit.source-service=payroll",
                        "spring.kafka.bootstrap-servers=localhost:9092")   // no entra.client-id
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalStateException.class);
                    assertThat(context.getStartupFailure().getMessage())
                            .contains("entra.client-id");
                });
    }

    @Test
    void auditEnabledFalseGivesANoOpClientAndNeedsNoConfig() {
        // No source-service, no bootstrap-servers — a disabled service should still
        // wire up cleanly and send() must be a harmless no-op.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        KafkaAutoConfiguration.class, AuditAutoConfiguration.class))
                .withPropertyValues("audit.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(AuditClient.class);
                    assertThatCode(() -> context.getBean(AuditClient.class)
                            .send(new com.company.audit.model.AuditEvent()))
                            .doesNotThrowAnyException();
                });
    }

    @Test
    void doesNotSuppressSpringBootDefaultKafkaBeans() {
        runner.run(context -> {
            // The crux of bean isolation: the SDK contributes NO ProducerFactory or
            // KafkaTemplate beans, so Boot's single defaults survive untouched.
            assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
            assertThat(context.getBeansOfType(ProducerFactory.class)).hasSize(1);

            // Boot's defaults, by the names KafkaAutoConfiguration gives them.
            assertThat(context).hasBean("kafkaTemplate");
            assertThat(context).hasBean("kafkaProducerFactory");
        });
    }

    @Test
    void withoutTheSdkBootStillHasItsDefaults() {
        // Sanity anchor: proves the assertion above is meaningful — Boot's defaults
        // exist on their own, and the SDK (previous test) leaves that count unchanged.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
                .run(context -> {
                    assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
                    assertThat(context.getBeansOfType(ProducerFactory.class)).hasSize(1);
                });
    }

    @Test
    void auditClientIsOverridable() {
        runner.withUserConfiguration(CustomClientConfig.class).run(context -> {
            assertThat(context).hasSingleBean(AuditClient.class);
            assertThat(context.getBean(AuditClient.class))
                    .isSameAs(CustomClientConfig.CUSTOM);
        });
    }

    @Configuration
    static class CustomClientConfig {
        @SuppressWarnings("unchecked")
        static final AuditClient CUSTOM = new AuditClient(
                mock(KafkaTemplate.class), new AuditProperties(), mock(Validator.class));

        @Bean
        AuditClient auditClient() {
            return CUSTOM;
        }
    }
}
