#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-game-platform}"
TARGET="${1:-game-room-server-0}"

printf 'Deleting pod %s/%s to trigger Raft failover...\n' "${NAMESPACE}" "${TARGET}"
kubectl delete pod "${TARGET}" -n "${NAMESPACE}"

printf 'Watch recovery with: kubectl get pods -n %s -w\n' "${NAMESPACE}"
