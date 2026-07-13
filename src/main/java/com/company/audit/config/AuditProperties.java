package com.company.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Properties teams set in their application.yml, e.g.:
 *
 * audit:
 *   source-service: payroll
 *   fail-on-error: false
 *
 * NOTE: the destination Kafka topic is intentionally NOT a property. It is owned by
 * the SDK ({@link com.company.audit.client.AuditClient#TOPIC}) so that every service
 * publishes to the same audit topic and no team can accidentally redirect events.
 */
@ConfigurationProperties(prefix = "audit")
public class AuditProperties {

    /**
     * Master on/off switch. When false the SDK provides a no-op AuditClient that
     * publishes nothing and creates no Kafka producer — useful for services that have
     * no Kafka, or for turning auditing off per environment. Default true.
     */
    private boolean enabled = true;

    /** Name of the emitting application. Required. Stamped onto every event. */
    private String sourceService;

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

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getSourceService() { return sourceService; }
    public void setSourceService(String sourceService) { this.sourceService = sourceService; }

    public boolean isFailOnError() { return failOnError; }
    public void setFailOnError(boolean failOnError) { this.failOnError = failOnError; }

    public Duration getSendTimeout() { return sendTimeout; }
    public void setSendTimeout(Duration sendTimeout) { this.sendTimeout = sendTimeout; }
}
