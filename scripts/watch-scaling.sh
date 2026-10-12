#!/usr/bin/env bash
# Scaling monitor: watch inference scale, one line per layer, every few seconds.
#
#   replicas   Ray Serve replicas (one vLLM process each) and their state
#   cores      NeuronCores Ray has handed out / NeuronCores in the Ray cluster
#   per node   where the RUNNING replicas live, by Trainium node
#   workers    Ray worker pods (created by KubeRay, each holding one Trainium
#              chip through a DRA ResourceClaim) and the node they run on
#
# Run it in its own terminal while scripts/load-test.sh or
# scripts/stepped-load.sh drives traffic.
#
# Usage: ./scripts/watch-scaling.sh [INTERVAL_SECONDS]
set -uo pipefail

INTERVAL="${1:-5}"

active_head() {
  local cluster
  cluster=$(kubectl get rayservice vllm-llm -o jsonpath='{.status.activeServiceStatus.rayClusterName}' 2>/dev/null)
  kubectl get pods -l "ray.io/cluster=${cluster},ray.io/node-type=head" \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1
}

PY=$(cat <<'PY'
import logging, ray
from ray import serve
from ray.util.state import list_actors, list_nodes
ray.init(address="auto", logging_level=logging.ERROR, log_to_driver=False)
node_ip = {n.node_id: n.node_ip for n in list_nodes()}
states = {}
for app in serve.status().applications.values():
    for dep in app.deployments.values():
        for state, n in dep.replica_states.items():
            states[str(state).split(".")[-1]] = states.get(str(state).split(".")[-1], 0) + n
total = ray.cluster_resources().get("neuron_cores", 0)
free = ray.available_resources().get("neuron_cores", 0)
per_ip = {}
for a in list_actors(filters=[("state", "=", "ALIVE")], detail=True, limit=1000):
    if a.class_name.startswith("ServeReplica:llm"):
        ip = node_ip.get(a.node_id, a.node_id[:8])
        per_ip[ip] = per_ip.get(ip, 0) + 1
print("STATES " + " ".join(f"{k}:{v}" for k, v in sorted(states.items())))
print(f"CORES {total - free:g}/{total:g}")
print("PERIP " + " ".join(f"{ip}={n}" for ip, n in sorted(per_ip.items())))
PY
)

while true; do
  HEAD=$(active_head)
  NOW=$(date +%H:%M:%S)
  if [ -z "$HEAD" ]; then
    echo "$NOW  (no active Ray head yet)"; sleep "$INTERVAL"; continue
  fi
  CLUSTER=$(kubectl get pod "$HEAD" -o jsonpath='{.metadata.labels.ray\.io/cluster}')

  # Worker pods: name -> IP, node, phase.
  WORKERS=$(kubectl get pods -l "ray.io/cluster=${CLUSTER},ray.io/node-type=worker" --no-headers \
    -o custom-columns=':metadata.name,:status.podIP,:spec.nodeName,:status.phase' 2>/dev/null)

  OUT=$(kubectl exec "$HEAD" -c ray-head -- python3 -c "$PY" 2>/dev/null)
  STATES=$(echo "$OUT" | sed -n 's/^STATES //p')
  CORES=$(echo "$OUT" | sed -n 's/^CORES //p')
  PERIP=$(echo "$OUT" | sed -n 's/^PERIP //p')

  # Map replica IPs to "node(N replicas)".
  PERNODE=""
  for pair in $PERIP; do
    ip=${pair%%=*}; n=${pair##*=}
    node=$(echo "$WORKERS" | awk -v ip="$ip" '$2==ip {print $3}' | sed 's/\.us-east-2\.compute\.internal//')
    PERNODE="$PERNODE ${node:-$ip}:$n"
  done

  WLINE=$(echo "$WORKERS" | awk 'NF {sub(/\.us-east-2\.compute\.internal/, "", $3); n=split($1, p, "-"); printf "%s@%s(%s) ", p[n], ($3=="<none>"?"unscheduled":$3), $4}')

  printf "%s  replicas[%s]  cores[%s]  per-node[%s ]\n          worker pods: %s\n" \
    "$NOW" "${STATES:-?}" "${CORES:-?}" "${PERNODE# }" "${WLINE:-none}"
  sleep "$INTERVAL"
done
