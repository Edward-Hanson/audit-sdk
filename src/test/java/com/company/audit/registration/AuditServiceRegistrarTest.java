package com.company.audit.registration;

import com.company.audit.config.EntraProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the full registration path against a stub HTTP server standing in for both
 * the Entra token endpoint and the audit service's {@code /register} endpoint.
 */
class AuditServiceRegistrarTest {

    private HttpServer server;
    private String baseUrl;

    private final AtomicReference<String> capturedAuthHeader = new AtomicReference<>();
    private final AtomicReference<String> capturedAppNameHeader = new AtomicReference<>();
    private final AtomicInteger registerCalls = new AtomicInteger();

    private volatile int tokenStatus = 200;
    private volatile int registerStatus = 200;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);

        // Entra token endpoint: /{tenant}/oauth2/v2.0/token
        server.createContext("/tenant-123/oauth2/v2.0/token", exchange -> {
            byte[] body = "{\"access_token\":\"tok-123\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                    .getBytes(StandardCharsets.UTF_8);
            respond(exchange, tokenStatus, tokenStatus == 200 ? body : "{}".getBytes());
        });

        // Audit service registration endpoint.
        server.createContext("/register", exchange -> {
            registerCalls.incrementAndGet();
            capturedAuthHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            capturedAppNameHeader.set(exchange.getRequestHeaders().getFirst("X-Audit-Source-Service"));
            respond(exchange, registerStatus, new byte[0]);
        });

        server.start();
        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private AuditServiceRegistrar registrar() {
        EntraProperties entra = new EntraProperties();
        entra.setClientId("client-abc");
        entra.setClientSecret("shhh");
        entra.setTenantId("tenant-123");
        entra.setAuthority(baseUrl);   // point Entra token calls at the stub

        HttpClient http = HttpClient.newHttpClient();
        EntraTokenClient tokenClient = new EntraTokenClient(http, new ObjectMapper(), entra);
        return new AuditServiceRegistrar(http, tokenClient, baseUrl, "payroll");
    }

    @Test
    void registersWithBearerTokenAndAppNameHeader() {
        registrar().register();

        assertThat(registerCalls.get()).isEqualTo(1);
        assertThat(capturedAuthHeader.get()).isEqualTo("Bearer tok-123");
        assertThat(capturedAppNameHeader.get()).isEqualTo("payroll");
    }

    @Test
    void failsWhenTokenRequestFails() {
        tokenStatus = 401;

        assertThatThrownBy(() -> registrar().register())
                .isInstanceOf(AuditRegistrationException.class)
                .hasMessageContaining("token");
        assertThat(registerCalls.get()).isZero(); // never reached the audit service
    }

    @Test
    void failsWhenRegistrationCallFails() {
        registerStatus = 500;

        assertThatThrownBy(() -> registrar().register())
                .isInstanceOf(AuditRegistrationException.class)
                .hasMessageContaining("registration failed");
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, byte[] body)
            throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
        exchange.close();
    }
}
