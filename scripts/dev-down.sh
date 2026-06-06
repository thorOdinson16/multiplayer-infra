#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

charts=(
  nginx
  notification-service
  analytics-service
  leaderboard-service
  replay-service
  reconnect-handler
  game-room-server
  matchmaking-service
  auth-service
  monitoring
  minio
  rabbitmq
  kafka
  etcd
  redis
  couchbase
)

for chart in "${charts[@]}"; do
  namespace="game-platform"
  case "${chart}" in
    couchbase|redis|etcd|kafka|rabbitmq|minio|nginx) namespace="infra" ;;
    monitoring) namespace="monitoring" ;;
  esac
  helm uninstall "${chart}" --namespace "${namespace}" >/dev/null 2>&1 || true
done

if [[ "${DELETE_PVCS:-false}" == "true" ]]; then
  kubectl delete pvc --all -n infra --ignore-not-found
  kubectl delete pvc --all -n game-platform --ignore-not-found
fi

printf 'Stack removed. Set DELETE_PVCS=true to also remove persistent volumes.\n'
