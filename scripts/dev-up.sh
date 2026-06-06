#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${ROOT_DIR}/scripts/verify-deps.sh"

kubectl create namespace infra --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace game-platform --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

charts=(
  couchbase
  redis
  etcd
  kafka
  rabbitmq
  minio
  monitoring
  auth-service
  matchmaking-service
  game-room-server
  reconnect-handler
  replay-service
  leaderboard-service
  analytics-service
  notification-service
  nginx
)

for chart in "${charts[@]}"; do
  namespace="game-platform"
  case "${chart}" in
    couchbase|redis|etcd|kafka|rabbitmq|minio|nginx) namespace="infra" ;;
    monitoring) namespace="monitoring" ;;
  esac
  helm upgrade --install "${chart}" "${ROOT_DIR}/helm/${chart}" --namespace "${namespace}"
done

printf 'Stack install/upgrade submitted. Watch with: kubectl get pods -A -w\n'
