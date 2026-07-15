package com.company.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Microsoft Entra (Azure AD) client-credentials configuration. Used to obtain an OAuth2
 * token that authenticates the one-time registration call to the audit service.
 *
 * <p>{@code client-id}, {@code client-secret} and {@code tenant-id} are required when
 * auditing is enabled — the SDK fails fast at startup if any is missing.
 *
 * <p>SECURITY: {@code entra.client-secret} must come from a secret store / environment
 * variable — never commit it to {@code application.yml} in source control.
 */
@ConfigurationProperties(prefix = "entra")
public class EntraProperties {

    /** Entra application (client) id. Required. Also used as the Kafka producer client.id. */
    private String clientId;

    /** Entra client secret. Required. Provide via env var / secret store. */
    private String clientSecret;

    /** Entra directory (tenant) id. Required. */
    private String tenantId;

    /**
     * OAuth2 scope for the client-credentials token. Optional — defaults to
     * {@code <client-id>/.default} when not set.
     */
    private String scope;

    /**
     * Entra authority base URL. Optional — defaults to the Azure public cloud. Override
     * for sovereign clouds (or a stub in tests).
     */
    private String authority = "https://login.microsoftonline.com";

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getClientSecret() { return clientSecret; }
    public void setClientSecret(String clientSecret) { this.clientSecret = clientSecret; }

    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }

    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }

    public String getAuthority() { return authority; }
    public void setAuthority(String authority) { this.authority = authority; }
}
