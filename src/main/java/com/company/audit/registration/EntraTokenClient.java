package com.company.audit.registration;

import com.company.audit.config.EntraProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Obtains an OAuth2 access token from Microsoft Entra using the client-credentials grant
 * ({@code client_id} + {@code client_secret} + {@code tenant_id}). Uses the JDK HTTP client
 * so the SDK pulls in no extra HTTP dependency.
 */
public class EntraTokenClient {

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final EntraProperties entra;
    private final String scope;

    public EntraTokenClient(HttpClient http, ObjectMapper mapper, EntraProperties entra, String scope) {
        this.http = http;
        this.mapper = mapper;
        this.entra = entra;
        this.scope = scope;
    }

    /** @return a bearer access token; throws {@link AuditRegistrationException} on failure. */
    public String fetchAccessToken() {
        String tokenUrl = trimTrailingSlash(entra.getAuthority())
                + "/" + entra.getTenantId() + "/oauth2/v2.0/token";

        String form = "grant_type=client_credentials"
                + "&client_id=" + enc(entra.getClientId())
                + "&client_secret=" + enc(entra.getClientSecret())
                + "&scope=" + enc(scope);

        HttpRequest request = HttpRequest.newBuilder(URI.create(tokenUrl))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new AuditRegistrationException(
                        "Entra token request failed (HTTP " + response.statusCode() + ")");
            }
            String token = mapper.readTree(response.body()).path("access_token").asText(null);
            if (!StringUtils.hasText(token)) {
                throw new AuditRegistrationException("Entra token response contained no access_token");
            }
            return token;
        } catch (IOException e) {
            throw new AuditRegistrationException("Failed to obtain Entra token", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AuditRegistrationException("Interrupted while obtaining Entra token", e);
        }
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
