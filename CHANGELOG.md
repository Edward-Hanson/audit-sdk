# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

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

[Unreleased]: https://github.com/Edward-Hanson/audit-sdk/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.1.1
[0.1.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.1.0
