# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

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
- **Distribution via JitPack**, sources + Javadoc jars, and a GitHub Actions build that
  runs the full test suite (including an embedded-Kafka serialization test) on push/PR.
- **Apache License 2.0.**

### Requirements

- Java 17+
- Spring Boot 3.x (built/tested against 3.3)
- A reachable Kafka broker (kafka-clients 3.x, transitive via `spring-kafka`)

[Unreleased]: https://github.com/Edward-Hanson/audit-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Edward-Hanson/audit-sdk/releases/tag/v0.1.0
