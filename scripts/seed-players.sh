#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-game-platform}"
SERVICE="${AUTH_SERVICE:-auth-service}"
LOCAL_PORT="${AUTH_LOCAL_PORT:-18080}"
PASSWORD="${SEED_PLAYER_PASSWORD:-password123}"

players=(
  "alice"
  "bob"
  "carol"
  "dave"
  "erin"
  "frank"
  "grace"
  "heidi"
)

cleanup() {
  if [[ -n "${pf_pid:-}" ]]; then
    kill "${pf_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

kubectl -n "${NAMESPACE}" port-forward "svc/${SERVICE}" "${LOCAL_PORT}:8080" >/tmp/auth-service-port-forward.log 2>&1 &
pf_pid=$!

for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:${LOCAL_PORT}/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl -fsS "http://127.0.0.1:${LOCAL_PORT}/ready" >/dev/null

for username in "${players[@]}"; do
  status="$(curl -sS -o /tmp/seed-player-response.json -w "%{http_code}" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${username}\",\"password\":\"${PASSWORD}\"}" \
    "http://127.0.0.1:${LOCAL_PORT}/auth/register")"

  case "${status}" in
    201) printf 'created %s\n' "${username}" ;;
    409) printf 'exists  %s\n' "${username}" ;;
    *)
      printf 'failed  %s status=%s body=%s\n' "${username}" "${status}" "$(tr -d '\n' </tmp/seed-player-response.json)" >&2
      exit 1
      ;;
  esac
done

printf 'Seeded %d demo players. Password: %s\n' "${#players[@]}" "${PASSWORD}"
