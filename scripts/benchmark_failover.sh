#!/bin/bash
# Repeatedly kill the game-room leader and measure recovery.
#   usage: scripts/benchmark_failover.sh [runs]     (default 10)
# Reports, per run and summarised (min/median/p95/max):
#   election_s  - kill -> a different instance holds the etcd leader key
#   resume_s    - kill -> game state in Redis advances past its pre-kill tick
# Polls every ~100ms, so resolution is far finer than test_failover_timing.sh.
set -uo pipefail

RUNS="${1:-10}"
MATCH_ID="${MATCH_ID:-test-match-001}"
REDIS_KEY="match:${MATCH_ID}:state"
compose_ps() { docker compose ps -q "$1" 2>/dev/null | head -1; }

ETCD=$(compose_ps etcd); REDIS=$(compose_ps redis)
[ -n "$ETCD" ] && [ -n "$REDIS" ] || { echo "docker compose stack is not running"; exit 1; }

leader() { docker exec "$ETCD" etcdctl get "/match/${MATCH_ID}/leader" --print-value-only 2>/dev/null | tr -d '\r\n'; }
tick()   {
  docker exec "$REDIS" redis-cli GET "$REDIS_KEY" 2>/dev/null |
    python3 -c "import sys,json; t=sys.stdin.read().strip(); print(json.loads(t).get('tick',-1) if t else -1)" 2>/dev/null || echo -1
}
now() { date +%s.%N; }

wait_for_leader() {
  for _ in $(seq 1 300); do L=$(leader); [ -n "$L" ] && { echo "$L"; return 0; }; sleep 0.1; done
  return 1
}

ELECTION=(); RESUME=()
echo "run  leader   election_s  resume_s"
for run in $(seq 1 "$RUNS"); do
  OLD=$(wait_for_leader) || { echo "no leader found"; exit 1; }
  sleep 3   # let the leader settle and produce ticks
  if [ "$OLD" = "room-2" ]; then SVC=game-room-2; else SVC=game-room; fi
  CID=$(compose_ps "$SVC")
  TICK0=$(tick)

  docker update --restart=no "$CID" >/dev/null 2>&1
  T0=$(now)
  docker kill "$CID" >/dev/null 2>&1

  E=""; R=""
  for _ in $(seq 1 400); do
    if [ -z "$E" ]; then
      NEW=$(leader)
      if [ -n "$NEW" ] && [ "$NEW" != "$OLD" ]; then E=$(python3 -c "print(round($(now)-$T0,2))"); fi
    fi
    if [ -n "$E" ] && [ -z "$R" ]; then
      T=$(tick)
      if [ "$T" -gt "$TICK0" ] 2>/dev/null; then R=$(python3 -c "print(round($(now)-$T0,2))"); break; fi
    fi
    sleep 0.1
  done

  docker update --restart=unless-stopped "$CID" >/dev/null 2>&1
  docker start "$CID" >/dev/null 2>&1
  printf "%-4s %-8s %-11s %s\n" "$run" "$OLD" "${E:-FAIL}" "${R:-FAIL}"
  [ -n "$E" ] && ELECTION+=("$E"); [ -n "$R" ] && RESUME+=("$R")
  sleep 12  # let the restarted replica rejoin as a follower
done

python3 - "${ELECTION[*]}" "${RESUME[*]}" "$RUNS" <<'PY'
import sys, statistics as st
def summ(label, vals, total):
    v = sorted(float(x) for x in vals.split())
    if not v:
        print(f"{label}: no successful runs"); return
    p95 = v[min(len(v) - 1, int(round(0.95 * (len(v) - 1))))]
    print(f"{label:11s} n={len(v)}/{total}  min={v[0]:.2f}  median={st.median(v):.2f}  p95={p95:.2f}  max={v[-1]:.2f}  (s)")
total = sys.argv[3]
print()
summ("election_s", sys.argv[1], total)
summ("resume_s", sys.argv[2], total)
PY
