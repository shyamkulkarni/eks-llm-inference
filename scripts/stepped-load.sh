#!/usr/bin/env bash
# Stepped load test: measures whether scaling actually improves throughput and latency.
#
# Holds each concurrency level for a fixed time and reports, per step:
# requests/s, output tokens/s, p50/p95 latency and errors. Run
# scripts/watch-scaling.sh in a second terminal to see replicas, NeuronCores
# and worker pods change while the numbers move.
#
# The load runs inside the cluster (a short-lived pod) against the in-cluster
# Service; a laptop port-forward can't sustain this much parallel traffic.
#
# Usage: ./scripts/stepped-load.sh ["CONCURRENCY:SECONDS ..."] [MAX_TOKENS]
#   default steps: "2:60 8:120 24:240"
set -euo pipefail

STEPS="${1:-2:60 8:120 24:240}"
MAX_TOKENS="${2:-128}"
ENDPOINT="http://vllm-llm-serve-svc.default.svc.cluster.local:8000/v1/chat/completions"
POD="stepped-load-$(date +%s)"

read -r -d '' PY <<'PY' || true
import json, os, statistics, threading, time, urllib.request

url, max_tokens = os.environ["ENDPOINT"], int(os.environ["MAX_TOKENS"])
body = json.dumps({
    "model": "Qwen/Qwen2.5-1.5B-Instruct",
    "messages": [{"role": "user", "content": "Write a short story about a Kubernetes cluster that learns to scale itself."}],
    "max_tokens": max_tokens,
}).encode()

def one():
    t0 = time.time()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            tokens = json.load(r)["usage"]["completion_tokens"]
        return time.time() - t0, tokens, None
    except Exception as e:
        return time.time() - t0, 0, type(e).__name__

print(f"{'step':>4} {'conc':>4} {'secs':>4} {'reqs':>5} {'req/s':>6} {'tok/s':>7} {'p50 s':>6} {'p95 s':>6} {'errors':>6}", flush=True)
for i, step in enumerate(os.environ["STEPS"].split(), 1):
    conc, secs = (int(x) for x in step.split(":"))
    results, lock, stop = [], threading.Lock(), time.time() + secs
    def worker():
        while time.time() < stop:
            r = one()
            with lock:
                results.append(r)
    threads = [threading.Thread(target=worker) for _ in range(conc)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.time() - start
    ok = [r for r in results if r[2] is None]
    lat = sorted(r[0] for r in ok) or [0]
    p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
    print(f"{i:>4} {conc:>4} {secs:>4} {len(ok):>5} {len(ok)/elapsed:>6.2f} {sum(r[1] for r in ok)/elapsed:>7.1f} "
          f"{statistics.median(lat):>6.2f} {p95:>6.2f} {len(results)-len(ok):>6}", flush=True)
PY

echo "Stepped load (concurrency:seconds) = ${STEPS}, max_tokens=${MAX_TOKENS}"
echo "Watch it scale in another terminal:  ./scripts/watch-scaling.sh"
kubectl run "${POD}" --rm -i --restart=Never --quiet \
  --image=public.ecr.aws/docker/library/python:3.12-alpine \
  --overrides='{"spec":{"nodeSelector":{"role":"system"}}}' \
  --env="ENDPOINT=${ENDPOINT}" --env="MAX_TOKENS=${MAX_TOKENS}" --env="STEPS=${STEPS}" \
  -- python3 -u -c "${PY}"
