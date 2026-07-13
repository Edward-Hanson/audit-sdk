package com.company.audit.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cross-version regression guard for {@link AuditAutoConfiguration#invokeBuildProducerProperties}.
 *
 * <p>{@code KafkaProperties.buildProducerProperties} is the only Spring Boot API whose
 * signature changes across the 3.x line, and it's the reason the SDK reflects over the
 * running Boot version instead of compiling a fixed call:
 * <ul>
 *   <li>Spring Boot 3.0 / 3.1 — {@code buildProducerProperties()} (no-arg; later removed)</li>
 *   <li>Spring Boot 3.2+     — {@code buildProducerProperties(SslBundles)}</li>
 *   <li>Spring Boot 3.2 / 3.3 — both present (no-arg deprecated but still there)</li>
 * </ul>
 * The stubs below reproduce each of those {@code KafkaProperties} shapes so we prove the
 * resolver picks and invokes the right overload on every version — without needing a
 * second Spring Boot on the classpath. (The real 3.3 path is exercised end-to-end by
 * {@code AuditSerializationIntegrationTest}.)
 */
class BuildProducerPropertiesCompatTest {

    /** Spring Boot 3.0 / 3.1 shape: only the no-arg overload. */
    static class LegacyKafkaProperties {
        public Map<String, Object> buildProducerProperties() {
            Map<String, Object> props = new HashMap<>();
            props.put("bootstrap.servers", "legacy:9092");
            props.put("overload", "no-arg");
            return props;
        }
    }

    /** Spring Boot 3.2+ shape: only the SslBundles overload (Object stands in for SslBundles). */
    static class ModernKafkaProperties {
        public Map<String, Object> buildProducerProperties(Object sslBundles) {
            Map<String, Object> props = new HashMap<>();
            props.put("bootstrap.servers", "modern:9092");
            props.put("overload", "ssl-bundles");
            props.put("sslBundlesWasNull", sslBundles == null);
            return props;
        }
    }

    /** Spring Boot 3.2 / 3.3 shape: both overloads present. */
    static class DualKafkaProperties {
        public Map<String, Object> buildProducerProperties() {
            return Map.of("bootstrap.servers", "dual-noarg:9092");
        }

        public Map<String, Object> buildProducerProperties(Object sslBundles) {
            return Map.of("bootstrap.servers", "dual-ssl:9092");
        }
    }

    /** A type with no such method — an unsupported/foreign version. */
    static class UnsupportedKafkaProperties {
    }

    @Test
    void resolvesNoArgOverload_springBoot30And31() {
        Map<String, Object> props =
                AuditAutoConfiguration.invokeBuildProducerProperties(new LegacyKafkaProperties());

        assertThat(props).containsEntry("overload", "no-arg");
        assertThat(props).containsEntry("bootstrap.servers", "legacy:9092");
    }

    @Test
    void resolvesSslBundlesOverloadAndPassesNull_springBoot32Plus() {
        Map<String, Object> props =
                AuditAutoConfiguration.invokeBuildProducerProperties(new ModernKafkaProperties());

        assertThat(props).containsEntry("overload", "ssl-bundles");
        assertThat(props).containsEntry("sslBundlesWasNull", true); // null passed for the param
        assertThat(props).containsEntry("bootstrap.servers", "modern:9092");
    }

    @Test
    void resolvesWhenBothOverloadsPresent_springBoot32And33() {
        Map<String, Object> props =
                AuditAutoConfiguration.invokeBuildProducerProperties(new DualKafkaProperties());

        // Either overload is valid; both return usable producer properties.
        assertThat(props).containsKey("bootstrap.servers");
    }

    @Test
    void throwsClearErrorWhenNoOverloadExists() {
        assertThatThrownBy(() ->
                AuditAutoConfiguration.invokeBuildProducerProperties(new UnsupportedKafkaProperties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("buildProducerProperties");
    }
}
