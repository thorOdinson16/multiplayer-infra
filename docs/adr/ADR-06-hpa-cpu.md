# ADR-06: CPU Utilization as HPA Trigger in v1 (Known Simplification)

---

## Context

The platform must scale game room pods horizontally in response to matchmaking demand (NFR-S-01). Kubernetes HPA can scale on:

- Built-in resource metrics: CPU and memory utilization
- Custom metrics: any value exposed via the Kubernetes custom metrics API (e.g., RabbitMQ queue depth via KEDA)

The semantically correct scaling signal is **matchmaking queue depth** — when more players are waiting to be matched than can be served by existing game room capacity, new pods should spin up. CPU utilization on game room pods is a proxy: it rises as active matches increase, which correlates with demand but lags behind it.

## Decision

In v1, configure HPA to scale game room pods when **average CPU utilization exceeds 70%**. This is a known simplification.

A v1.1 revision will replace or augment the CPU signal with a custom metric derived from RabbitMQ matchmaking queue depth, using **KEDA (Kubernetes Event-Driven Autoscaler)**.

## Rationale for v1 Simplification

CPU-based HPA requires no additional tooling — it is built into Kubernetes and works without a custom metrics adapter. For v1, where the goal is demonstrating that autoscaling works at all, CPU is sufficient.

The 70% threshold is set conservatively to provide headroom before pods become saturated. At 20 TPS per game room with up to 8 players, a fully loaded game room pod will drive CPU meaningfully, making the metric non-trivial.

## Why CPU is the Wrong Long-Term Signal

CPU is a **lagging indicator**. By the time CPU rises to 70%, game rooms are already loaded. A surge of 50 simultaneous matchmaking requests would not immediately raise CPU — it would only do so once those matches start. New pods would spin up *after* the surge, not *in anticipation of it*.

Matchmaking queue depth is a **leading indicator**: it rises the moment players enter the queue, before any game room is started. Scaling on queue depth allows pods to be provisioned in parallel with matchmaking, so rooms are ready when lobbies are assembled.

## Planned v1.1 Approach

KEDA scales Kubernetes workloads based on external event sources including RabbitMQ queue depth. The v1.1 implementation will:

1. Deploy KEDA in the `infra` namespace
2. Define a `ScaledObject` targeting the game room Deployment
3. Configure the RabbitMQ trigger on the `matchmaking.requests` queue
4. Set a target queue depth threshold (e.g., scale up when queue depth > 5 unprocessed messages)

This makes the scaling signal semantically correct and proactive.

## Consequences

- v1 HPA configuration is simple: one CPU metric, one threshold
- v1 may exhibit delayed scale-up under sudden demand surges — acceptable for development and demo purposes
- NFR-S-05 documents this as a known simplification so reviewers and interviewers are not misled
- v1.1 requires KEDA as an additional cluster dependency

## Alternatives Rejected for v1

| Alternative | Reason rejected |
|-------------|----------------|
| Queue-depth HPA (KEDA) immediately | Adds KEDA as a dependency before the core services are built; premature complexity |
| Memory-based HPA | Game room memory usage is more predictable than CPU and less correlated with load; worse proxy |
| Manual scaling | Violates NFR-S-01; not demonstrable as autonomous |