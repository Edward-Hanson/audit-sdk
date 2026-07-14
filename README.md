# Audit SDK — Spring Boot Starter

A thin Spring Boot starter that lets any service publish audit events to Kafka
with **SDK-side validation**. No schema registry: the SDK validates the event
before sending, then serializes it to JSON and produces it to Kafka.

## Requirements

- **Java 17+**
- **Spring Boot 3.x** — any 3.x version. The SDK adapts to your app's Spring Boot
  version (built against 3.3, verified to run on 3.1+); it does not pin one for you.
- **Kafka broker** reachable from your app; **kafka-clients 3.x** (arrives
  transitively via `spring-kafka` — see [Kafka client on the classpath](#kafka-client-on-the-classpath))

## What it does

1. Stamps SDK-owned fields: `sourceService` (from config), `eventId` (UUID,
   for consumer-side dedup), and `timestamp` (if unset).
2. Validates required fields and types before sending, and throws a clear
   `AuditValidationException` listing exactly what's wrong.
3. Produces to Kafka asynchronously with an **idempotent, acks=all** producer
   and retries. Events are sent with no key, so Kafka's sticky partitioner
   spreads them evenly across the topic's partitions (per-entity ordering is
   not required).
4. By default a failed send does **not** break your business action, and `send()`
   does not block the caller (a broker outage caps the caller's wait at ~2s via
   `max.block.ms`; retries continue off-thread). Set `audit.fail-on-error=true` to
   instead block up to `audit.send-timeout` for the broker ack and rethrow the
   failure into the caller — see below.
5. **Transaction-aware:** if `send()` is called inside an active Spring transaction,
   the publish is deferred until the transaction **commits**. If it rolls back, the
   event is never sent — no audit records for work that didn't happen. With no active
   transaction, the event is published immediately.

## What it does NOT do

- No schema registry. Structural guarantees come from this SDK only. A service
  that bypasses the SDK and writes raw bytes to the topic is not validated.
- No delivery guarantee beyond the producer's retries. If Kafka is unreachable
  for longer than `delivery.timeout.ms`, the event is logged and dropped (unless
  `audit.fail-on-error=true`). If you need zero loss during outages, add an
  outbox in your app.

## Install

The SDK is distributed via **[JitPack](https://jitpack.io/#Edward-Hanson/audit-sdk)** —
open source, **no credentials required**. Add the JitPack repository and the dependency.
The `version` is a released tag of this repo (e.g. `v0.1.0`), or `master-SNAPSHOT`
for the latest commit.

**Maven** (`pom.xml`):

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
    <version>v0.2.0</version>
</dependency>
```

> `<repositories>` must be a direct child of `<project>` — a sibling of
> `<dependencies>`, **not** nested inside it. A misplaced repository is silently
> ignored, and Maven then reports the artifact "not found in central".

<details>
<summary>Gradle</summary>

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.Edward-Hanson:audit-sdk:v0.2.0")
}
```
</details>

The first time anyone requests a version, JitPack builds this repo on demand (see
`jitpack.yml`) and caches the result; subsequent pulls are instant. Build status and
available versions: https://jitpack.io/#Edward-Hanson/audit-sdk

### Tracking the latest `master` (snapshots)

Released tags (`v0.1.0`, …) are **immutable** — recommended for production. To instead
follow the latest `master` and pick up new commits when you rebuild, depend on
`master-SNAPSHOT` and tell Maven to always re-check JitPack for a fresh build:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.Edward-Hanson</groupId>
    <artifactId>audit-sdk</artifactId>
    <version>master-SNAPSHOT</version>
</dependency>
```

With `updatePolicy=always`, each build re-checks JitPack; combined with JitPack
rebuilding `master-SNAPSHOT` on every push, a rebuild picks up your latest `master`.
Force it any time with `mvn -U`.

### Kafka client on the classpath

The SDK depends on `spring-kafka` (compile scope), which brings `kafka-clients`
onto your app's classpath transitively — so a normal Spring Boot service needs no
extra Kafka dependency to use this starter. (The `kafka-clients` entry in the SDK's
own POM is marked `provided`; that only affects how the SDK itself is built and does
**not** remove `kafka-clients` from your app — it still arrives via `spring-kafka`.)

If your app already manages a specific Kafka version (directly or via
`spring-boot-dependencies`), that version wins through normal Maven/Gradle
resolution; the SDK does not pin one for you. Supported: kafka-clients 3.x.

## Configure

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

audit:
  enabled: true               # optional (default: true) — false = no-op, no Kafka needed
  source-service: payroll     # required (when enabled) — identifies this app
  fail-on-error: false        # optional (default: false)
  send-timeout: 10s           # optional (default: 10s) — only used when fail-on-error=true
```

Set `audit.enabled=false` to turn the SDK into a no-op: `AuditClient` is still
injectable and `send(...)` calls are safe, but nothing is published and **no Kafka
producer is created** — so a service with no Kafka can keep the SDK on its classpath
without errors. Handy for local dev, tests, or environments where auditing is off.

### Transactions

Call `send()` from inside your `@Transactional` business method. The SDK detects the
active transaction and holds the publish until **after commit** (via
`TransactionSynchronization`), so a rollback emits nothing:

```java
@Transactional
public void changeSalary(...) {
    repository.save(...);          // business write
    auditClient.send(event);       // published only if this transaction commits
}                                  // rollback here → no audit event
```

Validation still runs synchronously at the `send()` call, so a malformed event fails
fast regardless of the transaction. One consequence: because the publish happens
*after* the commit, `fail-on-error=true`'s blocking/rethrow behavior does **not** apply
inside a transaction — the business data is already durable and can't be rolled back by
an audit failure, so the deferred publish is best-effort. Use `fail-on-error=true` for
non-transactional call sites where you genuinely want the action to fail if the audit
event can't be sent.

### fail-on-error: two modes

- **`false` (default) — best-effort, non-blocking.** `send()` returns immediately;
  the record is buffered and delivered on Kafka's I/O threads. Any failure (broker
  down, timeout, serialization) is logged and swallowed — your business action is
  never affected. The caller can still briefly block up to `max.block.ms` (~2s) if
  cluster metadata isn't yet available, but not for the full delivery timeout.
- **`true` — strict, blocking.** `send()` waits up to `send-timeout` for the broker
  to acknowledge the event. If it isn't acknowledged in time (or fails), `send()`
  throws `IllegalStateException`, which propagates into your caller so the business
  action can fail/roll back. Note: because the producer keeps retrying up to
  `delivery.timeout.ms`, an event may still be delivered *after* a `send-timeout`
  gives up — the consumer's `eventId` dedup handles that duplicate safely. Keep
  `send-timeout` below your request/transaction timeout.

The destination topic is **not configurable** — it is fixed by the SDK
(`audit_service`) so every service publishes to the same governed topic and no
team can redirect events. There is no `audit.topic` property.

## Use

```java
@Service
public class SalaryService {

    private final AuditClient auditClient;

    public SalaryService(AuditClient auditClient) {
        this.auditClient = auditClient;
    }

    public void changeSalary(Employee e, BigDecimal oldSalary, BigDecimal newSalary) {
        // ... perform the business change ...

        auditClient.send(AuditEventBuilder.builder()
                .userName("jane.admin")
                .userId(42L)
                .action(AuditAction.UPDATE)
                .entityType("EMPLOYEE")
                .entityId(e.getId().toString())
                .entityName(e.getName())
                .organizationId(1)                       // optional
                .details("Adjusted annual salary")
                .oldPayload(Map.of("salary", oldSalary)) // state before
                .newPayload(Map.of("salary", newSalary)) // state after
                .payloadDifference(Map.of("salary", newSalary.subtract(oldSalary)))
                .build());
    }
}
```

## Fields

**Required (validated by the SDK):** `userName`, `userId`, `entityId`, `entityType`,
`action`.

**Auto-filled by the SDK:** `eventId`, `sourceService`, `timestamp`.

**Optional:** `organizationId`, `entityName`, `details`, `oldPayload` (state before),
`newPayload` (state after), `payloadDifference` (the delta).

### `action` — controlled vocabulary

`action` is the `AuditAction` enum (auditing targets state/data changes; reads are not
audited). Allowed values:

```
CREATE, UPDATE, DELETE,
ARCHIVE, UNARCHIVE,
ACTIVATE, DEACTIVATE,
SUBMIT, APPROVE, REJECT, DENY, CANCEL,
ASSIGN, UNASSIGN,
LOCK, UNLOCK, SUSPEND, RESTORE,
PUBLISH, UNPUBLISH,
GRANT, REVOKE
```

Need another state-change action? Add it to `AuditAction` and release a new SDK
version so the vocabulary stays governed across all services.

## Notes for the consumer (audit service)

- Deduplicate on `eventId` (unique constraint / upsert) — consumption is
  at-least-once, so duplicates will occur.
- Commit the Kafka offset only after the DB write succeeds, so a crash re-reads
  rather than loses an event.
- The value is plain JSON; deserialize into your own `AuditEvent` shape.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
