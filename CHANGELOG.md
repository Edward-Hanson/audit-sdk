# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

## [0.6.0] - 2026-07-20

Breaking — a new required property, and permission enforcement on the audit service.

### Added

- **Required `audit.scope`** — the OAuth2 scope the SDK requests from Entra for the
  audit-service API (typically `api://<audit-app-id>/.default`). It sets the token's
  audience and the app-role permissions it carries. The SDK fails fast at startup if
  it's missing (when enabled).

### Changed

- The Entra token is now requested with `audit.scope` (was an optional `entra.scope`
  defaulting to `<client-id>/.default`). **Removed `entra.scope`.**
- The audit service now enforces app-role permissions from the token's `roles` claim:
  **`audit.register`** to call `POST /register`, **`audit.read`** to query `/graphql`.
  Grant these app roles to each client app in Entra.

### Migration

- Add `audit.scope` to every service's config, and grant the client app the
  `audit.register` (and `audit.read`, if it queries) app roles on the audit-service
  app registration in Entra.

## [0.5.0] - 2026-07-16

Breaking — a config rename and two new event fields.

### Changed

- **Renamed `audit.source-service` → `audit.display-name`** (compulsory). It's the
  human-friendly application name, stamped as the event's `sourceService`, sent as the
  app name at registration, and shown in the audit UI. Users may change it over time.

### Added

- **`clientId` on `AuditEvent`** — stamped from the compulsory `entra.client-id` (the
  verified identity). The audit service uses it to attribute and gate events.
- **`metadata` on `AuditEvent`** — a free-form `Map` for services to attach extra fields
  (`.metadata(Map.of(...))` on the builder).

### Migration

- Rename `audit.source-service` → `audit.display-name` in every service's config.

## [0.4.0] - 2026-07-15

Breaking configuration change — the audit destination is no longer baked into the SDK.

### Changed

- **The audit broker and topic are now required config, not hardcoded.** Removed the
  hardcoded `AuditClient.TOPIC` constant. The SDK now publishes to `audit.kafka.topic`
  on the dedicated broker `audit.kafka.servers` — **independent of the app's
  `spring.kafka.*`** — so each environment (dev/stage/prod) points at its own
  cluster/topic and can't cross-publish.
- **Fail-fast at startup** (when `audit.enabled=true`) if any of these are missing:
  `audit.kafka.servers`, `audit.kafka.topic`, `audit.source-service`, `entra.client-id`.
  (`source-service` is now checked at startup, not only per-send.)

### Added

- **Startup registration with the audit service.** When enabled, the SDK registers the
  application once at startup, before any events are published: it obtains a Microsoft
  Entra client-credentials token and calls `POST {audit.url}/register` with it as a
  `Bearer` header (no body). If registration fails, the application fails to start.
  Uses the JDK HTTP client (no new dependency).
- **New required properties** (when enabled): `entra.client-secret`, `entra.tenant-id`,
  `audit.url` (join the existing `entra.client-id`). Optional: `entra.scope`
  (default `<client-id>/.default`) and `entra.authority` (default Azure public cloud).
- **`audit.kafka.properties`** — optional pass-through map for producer settings
  (e.g. `security.protocol`, `sasl.*`) so a secured audit broker can be configured
  per environment without code changes.

### Removed

- Internal `KafkaProperties.buildProducerProperties(...)` reflection (and its
  compatibility test) — the SDK no longer reads the app's `spring.kafka.*`, so the
  version-sensitive API is no longer used.

### Migration

- Add `audit.kafka.servers`, `audit.kafka.topic`, `audit.url`, `entra.client-secret`,
  and `entra.tenant-id` to every service using the SDK (per environment), or it will
  fail to start once upgraded. The SDK no longer uses `spring.kafka.bootstrap-servers`
  for audit.
- The audit service must expose `POST /register` (validating the Entra bearer token)
  before consumers upgrade — the SDK calls it at startup and fails to start if it 404s
  or errors.

## [0.3.0] - 2026-07-15

### Added

- **Required `entra.client-id` property.** Read from `application.yml`/`.properties`
  and applied as the Kafka producer's `client.id`. The SDK **fails fast at startup**
  with a clear error if it's missing (when auditing is enabled). Not required when
  `audit.enabled=false`.

### Migration

- Add `entra.client-id: <your-entra-client-id>` to every service using the SDK, or it
  will fail to start once upgraded.

## [0.2.0] - 2026-07-14

Breaking schema changes — upgrade requires code changes at call sites and a matching
update on the audit-consumer side.

### Changed

- **`action` is now the `AuditAction` enum**, not a free String — restricting audit
  events to a governed vocabulary of state/data-change actions: `CREATE, UPDATE,
  DELETE, ARCHIVE, UNARCHIVE, ACTIVATE, DEACTIVATE, SUBMIT, APPROVE, REJECT, DENY,
  CANCEL, ASSIGN, UNASSIGN, LOCK, UNLOCK, SUSPEND, RESTORE, PUBLISH, UNPUBLISH, GRANT,
  REVOKE`. (Reads are not audited.)
- **Payload fields renamed** for clarity:
  - `currentPayload` → `newPayload` (state after the change)
  - `changedPayload` → `oldPayload` (state before the change)
  - `payload` → `payloadDifference` (the delta)
- **`organizationId` is now optional** (was required).

### Migration

- Replace `.action("SOME_STRING")` with `.action(AuditAction.X)`.
- Rename builder calls: `.currentPayload(..)` → `.newPayload(..)`,
  `.changedPayload(..)` → `.oldPayload(..)`, `.payload(..)` → `.payloadDifference(..)`.
- The JSON on the topic changes accordingly (`action` is an enum name; renamed payload
  keys) — update the audit-consumer's mapping.

## [0.1.1] - 2026-07-14

### Changed

- **Audit topic renamed** from `audit-service` to `audit_service` to match the
  provisioned Kafka topic. Upgrade to `v0.1.1`; events now publish to `audit_service`.
  (Consumers of the audit topic must read from `audit_service`.)

## [0.1.0] - 2026-07-13

First public release — a thin Spring Boot starter for publishing validated audit
events to Kafka. Drop it in, inject `AuditClient`, and send; no Kafka boilerplate,
no schema registry.

### Added

- **One-call auditing** via `AuditClient.send(...)` with a fluent `AuditEventBuilder`.
  Auto-configured — a consuming app only sets `audit.source-service`.
- **SDK-side validation** of required fields before send; invalid events throw
  `AuditValidationException` listing exactly what is wrong.
- **Fixed, governed topic** (`audit-service`) owned by the SDK — not configurable, so
  no team can redirect events.
- **Even partition distribution** — events are sent with no key, letting Kafka's
  sticky partitioner spread load across partitions.
- **Reliable producer** — idempotent, `acks=all`, retries, with `max.block.ms`
  bounded so a broker outage cannot stall the caller.
- **Two failure modes** — best-effort by default (a failed audit send never breaks the
  business action); `audit.fail-on-error=true` blocks up to `audit.send-timeout` and
  propagates the failure to the caller.
- **Transaction awareness** — when called inside a Spring `@Transactional` method the
  publish is deferred until commit; on rollback nothing is emitted.
- **Bean isolation** — the SDK never overrides the host app's own Kafka
  `ProducerFactory` / `KafkaTemplate` beans.
- **Auto-stamped fields** — `eventId` (for consumer-side dedup), `sourceService`, and
  `timestamp`.
- **`audit.enabled` toggle** — set `false` for a no-op `AuditClient` that publishes
  nothing and creates no Kafka producer (for services with no Kafka, or to switch
  auditing off per environment). Default `true`.
- **Distribution via JitPack** — no credentials required. Coordinate:
  `com.github.Edward-Hanson:audit-sdk:<version>` with the `https://jitpack.io`
  repository. Ships sources + Javadoc jars, a Maven Wrapper (3.9.9) so JitPack builds
  with a compatible Maven, and a GitHub Actions build that runs the full test suite
  (including an embedded-Kafka serialization test) on push/PR.
- **Apache License 2.0.**

### Requirements

- Java 17+
- Spring Boot 3.x — any 3.x version (built against 3.3, runs on 3.1+; the SDK adapts
  to the app's Boot version rather than pinning one)
- A reachable Kafka broker (kafka-clients 3.x, transitive via `spring-kafka`)

### Install

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.Edward-Hanson</groupId>
  <artifactId>audit-sdk</artifactId>
  <version>v0.1.1</version>
</dependency>
```

[Unreleased]: https://github.com/Edward-Hanson/audit-sdk/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.6.0
[0.5.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.5.0
[0.4.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.4.0
[0.3.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.3.0
[0.2.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.2.0
[0.1.1]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.1.1
[0.1.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.1.0
