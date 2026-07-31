package com.company.audit.registration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Registers this application with the audit service exactly once, at startup, before any
 * events are published. Flow:
 * <ol>
 *   <li>obtain an Entra client-credentials token ({@link EntraTokenClient}), then</li>
 *   <li>{@code POST {audit.url}/register} with that token as a {@code Bearer} header (no body).</li>
 * </ol>
 *
 * <p>This is a plain class with a single {@link #register()} method. Each starter drives it
 * from an {@code ApplicationRunner} so that a registration failure propagates and the
 * application fails to start (fail-fast) — auditing is mandatory when enabled, so a service
 * never runs un-registered.
 */
public class AuditServiceRegistrar {

    private static final Logger log = LoggerFactory.getLogger(AuditServiceRegistrar.class);

    /** Header carrying the app name (the SDK's {@code audit.display-name}) to register. */
    public static final String APP_NAME_HEADER = "X-Audit-Source-Service";

    private final HttpClient http;
    private final EntraTokenClient tokenClient;
    private final String auditUrl;
    private final String sourceService;

    public AuditServiceRegistrar(HttpClient http, EntraTokenClient tokenClient,
                                 String auditUrl, String sourceService) {
        this.http = http;
        this.tokenClient = tokenClient;
        this.auditUrl = auditUrl;
        this.sourceService = sourceService;
    }

    /** Performs the token fetch + registration call. Throws on any failure. */
    public void register() {
        String token = tokenClient.fetchAccessToken();
        String registerUrl = trimTrailingSlash(auditUrl) + "/register";

        HttpRequest request = HttpRequest.newBuilder(URI.create(registerUrl))
                .timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token)
                .header(APP_NAME_HEADER, sourceService)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new AuditRegistrationException(
                        "Audit service registration failed at " + registerUrl
                                + " (HTTP " + response.statusCode() + ")");
            }
            log.info("Registered with audit service at {}", registerUrl);
        } catch (IOException e) {
            throw new AuditRegistrationException(
                    "Failed to reach audit service for registration at " + registerUrl, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AuditRegistrationException("Interrupted while registering with audit service", e);
        }
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
