# Multiplayer Infrastructure

Kubernetes-native multiplayer backend demo covering authentication, matchmaking, Raft-backed game rooms, reconnect state, replay archival, leaderboard updates, analytics, and observability.

## Requirements

- Docker
- Go 1.22+
- kubectl connected to a local cluster such as minikube
- Helm 3
- curl

Check local tools:

```bash
make verify
```

## Local Deploy

```bash
make dev-up
kubectl get pods -A -w
```

Seed demo players:

```bash
make seed-players
```

Default seeded password is `password123`. Override with `SEED_PLAYER_PASSWORD=...`.

Tear down without deleting PVCs:

```bash
make dev-down
```

Delete PVCs too:

```bash
DELETE_PVCS=true make dev-down
```

## Demo Flow

1. Login: port-forward `auth-service` and call `/auth/login` with a seeded user.
2. Queue matchmaking: POST player data to `/matchmaking/queue`.
3. Observe game room provisioning: `kubectl get statefulsets,svc -n game-platform`.
4. Connect client/WebSocket to `/game/?playerId=...&username=...`.
5. Trigger failover: `make fault-inject` deletes `game-room-server-0`.
6. Reconnect: call reconnect-handler while Redis still has `match:{matchId}:players` and `match:{matchId}:state`.
7. Replay: end the match and fetch `/replay/{matchId}` after replay-service archives to MinIO.
8. Leaderboard: query `/leaderboard` after `match_end` lifecycle events.
9. Observability: open Jaeger and Grafana from the monitoring namespace.

## Acceptance Coverage

- AC-01: `scripts/seed-players.sh` plus `/auth/register` create players for login demos.
- AC-02: `scripts/fault-inject.sh` deletes the first Raft pod to demonstrate leader election.
- AC-03: game-room-server stores `match:{matchId}:players` and `match:{matchId}:state` in Redis.
- AC-04: replay-service archives buffered match events to MinIO on `match_end`.
- AC-05: leaderboard service consumes lifecycle events and queries indexed Couchbase docs.
- AC-06: game-room and replay-service emit OTLP traces through the OTel collector to Jaeger.
- AC-07: matchmaking RBAC can create both per-match StatefulSets and headless Services.
- AC-08: CI builds services and lints charts; run `make test` locally.
- AC-09: `gitops/` contains Argo CD `Application` manifests for apps and infrastructure.
- AC-10: Grafana provisions a platform overview dashboard and Prometheus datasource.
- AC-11: matchmaking exposes `matchmaking_broker_unavailable` on `/metrics`.
- AC-12: spectator broadcast uses the game broadcaster's synchronized spectator map.

## Useful Commands

```bash
make build
make test
make helm-lint
kubectl port-forward -n monitoring svc/grafana 3000:3000
kubectl port-forward -n monitoring svc/jaeger 16686:16686
kubectl port-forward -n monitoring svc/prometheus 9090:9090
```

## Notes

- The static Helm `game-room-server` StatefulSet uses `MATCH_ID=demo-match` for a coherent demo room.
- Matchmaking-created rooms set their own unique `MATCH_ID` from the provisioner.
- NGINX is not OTel-instrumented; traces start at the instrumented Go services.
