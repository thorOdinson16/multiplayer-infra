# ADR-02: Kafka and RabbitMQ as Complementary, Non-Competing Brokers

---

## Context

The platform needs to move data between services in two fundamentally different ways:

1. **High-throughput event logging** — every game tick produces movement events that must be recorded in order, retained for replay and analytics, and consumed independently by multiple downstream services at their own pace.
2. **Task dispatch** — matchmaking requests, notification jobs, and async background tasks need to be claimed by exactly one consumer, processed once, and acknowledged. Unprocessed tasks must not be silently dropped.

The question is whether one broker can serve both patterns, or whether two distinct brokers are warranted.

## Decision

Use **Apache Kafka** exclusively for the immutable ordered event log, and **RabbitMQ** exclusively for task queues and async job dispatch. The two brokers are never substituted for each other.

**Kafka owns:**
- `match.events` — movement and state transition events
- `match.telemetry` — analytics telemetry
- `match.lifecycle` — match start/end signals

**RabbitMQ owns:**
- `matchmaking.requests` — player matchmaking queue
- `matchmaking.expired` — dead-letter queue for expired requests
- `notifications.exchange` — fan-out for all notification event types

## Rationale

Kafka and RabbitMQ are not interchangeable. They are different tools built for different primitives:

| Property | Kafka | RabbitMQ |
|----------|-------|----------|
| Message model | Immutable log, offset-based | Queue, acknowledgement-based |
| Consumer model | Each consumer group reads independently; messages not removed on consume | Competing consumers; message removed on acknowledgement |
| Retention | Time/size based; messages retained regardless of consumption | Messages removed after acknowledgement |
| Ordering | Strict per partition | Per queue, not guaranteed across exchanges |
| Replay | Native (seek to offset) | Not supported |
| Use case fit | Event sourcing, audit log, stream processing | Task queues, RPC, work distribution |

Using Kafka for matchmaking would require implementing competing consumer semantics manually on top of consumer groups, which Kafka does not model natively. Using RabbitMQ for the event log would lose replay capability, independent multi-consumer reads, and ordered retention — all of which are required.

## Consequences

- Two brokers to operate, monitor, and secure
- Both brokers require authentication credentials injected via Kubernetes Secrets
- Observability must cover both: RabbitMQ queue depth and Kafka consumer lag are distinct metrics
- Engineers working on the codebase must understand which broker to use for which pattern — this ADR serves as the reference

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| Kafka only | Competing consumer / exactly-once task semantics are unnatural in Kafka; matchmaking and notification dispatch would require significant workaround code |
| RabbitMQ only | No native replay, no offset-based multi-consumer reads, unsuitable for high-throughput event log |
| Redis Streams | Capable of both patterns at small scale but lacks Kafka's durability guarantees and RabbitMQ's routing flexibility; adds operational complexity without clear benefit |
| Single in-process queue | Not durable across pod restarts; not scalable across service replicas |