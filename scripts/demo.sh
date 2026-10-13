#!/usr/bin/env bash
# End-to-end demo: control plane + 3 (then 4) node agents on this machine.
# Walks through scheduling, routing, crash restart, health-check restart,
# stateful workloads with volumes, node failure + rescheduling, rolling update
# and autoscaling. Run via `make demo`.
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://127.0.0.1:7070
ROUTER=http://127.0.0.1:8080
DATA=.data
LOGS=$DATA/logs
ICCTL="./bin/icctl --api $API"
PIDS=()

bold() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

wait_until() { # desc timeout cmd...
  local desc=$1 timeout=$2; shift 2
  for _ in $(seq 1 $((timeout * 2))); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "timed out waiting for: $desc" >&2
  $ICCTL get instances >&2 || true
  exit 1
}

node_of() { $ICCTL get instances | awk -v i="$1" '$1==i {print $6}'; }
phase_of() { $ICCTL get instances | awk -v i="$1" '$1==i {print $3}'; }
node_status() { $ICCTL get nodes | awk -v n="$1" '$1==n {print $2}'; }

rm -rf "$DATA" && mkdir -p "$LOGS"

bold "Starting control plane (API :7070, router :8080)"
./bin/instacloud-controlplane --data-dir "$DATA/controlplane" --heartbeat-grace 6s \
  --stateful-fence-delay 8s --autoscale-interval 3s --scale-down-window 20s \
  >"$LOGS/controlplane.log" 2>&1 &
PIDS+=($!)
wait_until "api" 10 curl -sf "$API/healthz"

declare -A AGENT_PID
start_agent() { # name zone port-base
  ./bin/instacloud-agent --name "$1" --zone "$2" --api "$API" --port-base "$3" \
    --cpu 2000 --memory 2048 --data-dir "$DATA" --volume-root "$DATA/volumes" --lease 5s \
    >"$LOGS/agent-$1.log" 2>&1 &
  AGENT_PID[$1]=$!
  PIDS+=($!)
}
bold "Starting 3 node agents in 3 zones"
start_agent node-1 zone-a 21000
start_agent node-2 zone-b 22000
start_agent node-3 zone-c 23000
wait_until "3 nodes" 10 bash -c "[ \$($ICCTL get nodes | grep -c ' Ready ') -eq 3 ]"
$ICCTL get nodes

bold "1. Deploy a stateless service: 3 replicas, health-checked"
$ICCTL apply -f examples/web.json
$ICCTL wait web -timeout 30s
$ICCTL get instances
echo "-- requests through the router are spread across replicas:"
for _ in 1 2 3 4 5 6; do curl -s "$ROUTER/w/web/"; done

bold "2. Crash a replica -> agent restarts it with backoff"
curl -s -X POST "$ROUTER/w/web/crash"
sleep 1
$ICCTL get instances | awk 'NR==1 || /Failed/'
$ICCTL wait web -timeout 30s
$ICCTL get instances

bold "3. Make a replica fail health checks -> drained from router, restarted"
curl -s -X POST "$ROUTER/w/web/sick"
wait_until "unhealthy restart" 20 bash -c "[ \$($ICCTL get instances | awk '\$2==\"web\" {s+=\$5} END {print s}') -ge 2 ]"
$ICCTL wait web -timeout 30s
$ICCTL get instances

bold "4. Deploy a stateful workload: ordered ordinals, one volume each, anti-affinity"
$ICCTL apply -f examples/kv.json
$ICCTL wait kv -timeout 60s
$ICCTL get instances | awk 'NR==1 || $2=="kv"'
$ICCTL get volumes
curl -s -X PUT --data "hello-from-kv-1" "$ROUTER/w/kv-1/kv/greeting"
curl -s "$ROUTER/w/kv-1/kv/greeting"

bold "5. Add a 4th node, then kill the node running kv-1 (agent + its instances)"
start_agent node-4 zone-a 24000
wait_until "4 nodes" 10 bash -c "[ \$($ICCTL get nodes | grep -c ' Ready ') -eq 4 ]"
VICTIM=$(node_of kv-1)
echo "kv-1 runs on $VICTIM; killing it (SIGKILL, no graceful drain)"
kill -9 "${AGENT_PID[$VICTIM]}"
wait_until "$VICTIM NotReady" 20 bash -c "[ \"\$($ICCTL get nodes | awk -v n=$VICTIM '\$1==n {print \$2}')\" = NotReady ]"
$ICCTL get nodes
$ICCTL get instances
echo "-- stateless replicas are replaced right away; kv-1 waits for the fence delay, then moves with its volume"
wait_until "kv-1 rescheduled" 40 bash -c "n=\$($ICCTL get instances | awk '\$1==\"kv-1\" && \$3==\"Running\" && \$4==\"true\" {print \$6}'); [ -n \"\$n\" ] && [ \"\$n\" != $VICTIM ]"
$ICCTL wait web -timeout 30s
$ICCTL get instances
$ICCTL get volumes
echo "-- data written before the failure is still there on the new node:"
curl -s "$ROUTER/w/kv-1/kv/greeting"

bold "6. Rolling update (VERSION v1 -> v2) with autoscaling enabled"
$ICCTL apply -f examples/web-autoscale.json
$ICCTL wait web -timeout 60s
for _ in 1 2 3; do curl -s "$ROUTER/w/web/"; done

bold "7. Generate load (~100+ rps) -> autoscaler scales web up"
LOAD_SECS=${LOAD_SECS:-12}
end=$((SECONDS + LOAD_SECS))
LOAD_PIDS=()
for _ in $(seq 1 8); do
  ( while [ $SECONDS -lt $end ]; do curl -s -o /dev/null "$ROUTER/w/web/work?ms=5"; done ) &
  LOAD_PIDS+=($!)
done
wait_until "scale up" 40 bash -c "[ \$($ICCTL get workloads | awk '\$1==\"web\" {split(\$3,a,\"/\"); print a[2]}') -gt 2 ]"
wait "${LOAD_PIDS[@]}"
$ICCTL get workloads
grep '"msg":"autoscaled"' "$LOGS/controlplane.log" | tail -3

bold "8. Metrics (Prometheus text format)"
curl -s "$API/metrics" | grep -E '^instacloud_(instances|nodes|scheduler_attempts_total|instances_lost_total|autoscaler_decisions_total|router_requests_total)' | head -25

bold "Demo complete. Logs: $LOGS/"
