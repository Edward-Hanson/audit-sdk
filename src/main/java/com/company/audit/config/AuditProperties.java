package com.company.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Properties teams set in their application.yml, e.g.:
 *
 * audit:
 *   display-name: Payroll
 *   fail-on-error: false
 *   kafka:
 *     servers: broker1:9092,broker2:9092   # dedicated audit broker (per environment)
 *     topic: audit_service_dev             # dedicated audit topic (per environment)
 *
 * The destination (broker + topic) is intentionally NOT baked into the SDK — it must be
 * supplied per environment so dev/stage/prod can't cross-publish. When auditing is enabled
 * both {@code audit.kafka.servers} and {@code audit.kafka.topic} are required or the app
 * fails to start.
 */
@ConfigurationProperties(prefix = "audit")
public class AuditProperties {

    /**
     * Master on/off switch. When false the SDK provides a no-op AuditClient that
     * publishes nothing and creates no Kafka producer — useful for services that have
     * no Kafka, or for turning auditing off per environment. Default true.
     */
    private boolean enabled = true;

    /**
     * Human-friendly application name. Required when enabled. Stamped onto every event as
     * its {@code sourceService}, sent as the app name at registration, and shown in the
     * audit UI. Users may change it over time.
     */
    private String displayName;

    /**
     * Base URL of the audit service. Required when enabled. The SDK registers this
     * application once at startup via {@code POST {url}/register} (authenticated with an
     * Entra token) before any events are published.
     */
    private String url;

    /**
     * OAuth2 scope requested from Entra for the audit-service API. Required when enabled.
     * Determines the token's audience and the app-role permissions it carries (e.g.
     * {@code audit.register}, {@code audit.read}) — typically {@code api://<audit-app-id>/.default}.
     */
    private String scope;

    /**
     * If true, send() throws when the send to Kafka ultimately fails.
     * If false (default), failures are logged but do not disrupt the caller's
     * business action. Recommended: false — a failed audit log should not
     * break a salary change.
     */
    private boolean failOnError = false;

    /**
     * Only used when {@link #failOnError} is true: how long send() blocks the caller
     * waiting for the broker to acknowledge the event before giving up and throwing.
     * Ignored when failOnError is false (best-effort mode never blocks). Keep this
     * comfortably below any request/transaction timeout on the caller's side.
     */
    private Duration sendTimeout = Duration.ofSeconds(10);

    /** Dedicated audit Kafka settings (broker + topic). Required when enabled. */
    private final Kafka kafka = new Kafka();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }

    public boolean isFailOnError() { return failOnError; }
    public void setFailOnError(boolean failOnError) { this.failOnError = failOnError; }

    public Duration getSendTimeout() { return sendTimeout; }
    public void setSendTimeout(Duration sendTimeout) { this.sendTimeout = sendTimeout; }

    public Kafka getKafka() { return kafka; }

    /**
     * Dedicated audit Kafka configuration — kept separate from the host app's
     * {@code spring.kafka.*} so audit events go to their own cluster/topic per environment.
     */
    public static class Kafka {

        /** Bootstrap servers for the audit cluster (comma-separated). Required when enabled. */
        private String servers;

        /** Topic to publish audit events to. Required when enabled. */
        private String topic;

        /**
         * Optional extra producer properties passed straight through to the Kafka client —
         * e.g. {@code security.protocol}, {@code sasl.mechanism}, {@code sasl.jaas.config} for
         * a secured audit broker. SDK-owned settings (serializers, idempotence, acks, etc.)
         * always take precedence.
         */
        private Map<String, String> properties = new HashMap<>();

        public String getServers() { return servers; }
        public void setServers(String servers) { this.servers = servers; }

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public Map<String, String> getProperties() { return properties; }
        public void setProperties(Map<String, String> properties) { this.properties = properties; }
    }
}
