#!/usr/bin/env bash
# Burst load test: concurrent requests so Ray Serve autoscaling adds replicas.
#
# The load runs INSIDE the cluster, as a short-lived pod hitting the in-cluster
# Service. Don't drive concurrent load through `kubectl port-forward`: it is a
# single tunnel that drops connections after a few dozen parallel requests,
# which is indistinguishable from a server outage in the results.
#
# Watch it work in a second terminal:
#   ./scripts/watch-scaling.sh
#
# For per-step throughput and latency numbers, use scripts/stepped-load.sh.
#
# Usage: ./scripts/load-test.sh [CONCURRENCY] [TOTAL_REQUESTS] [MAX_TOKENS]
set -euo pipefail

CONCURRENCY="${1:-24}"
TOTAL="${2:-600}"
MAX_TOKENS="${3:-128}"
ENDPOINT="http://vllm-llm-serve-svc.default.svc.cluster.local:8000"
POD="load-test-$(date +%s)"

PAYLOAD="{\"model\":\"Qwen/Qwen2.5-1.5B-Instruct\",\"messages\":[{\"role\":\"user\",\"content\":\"Write a short story about a Kubernetes cluster that learns to scale itself.\"}],\"max_tokens\":${MAX_TOKENS}}"

echo "Firing ${TOTAL} requests at concurrency ${CONCURRENCY} from inside the cluster -> ${ENDPOINT}"

# The script runs in a pod; results stream back here. The pod is removed on exit.
kubectl run "${POD}" --rm -i --restart=Never --quiet \
  --image=public.ecr.aws/docker/library/alpine:3.20 \
  --overrides='{"spec":{"nodeSelector":{"role":"system"}}}' \
  --env="ENDPOINT=${ENDPOINT}" --env="PAYLOAD=${PAYLOAD}" \
  --env="CONCURRENCY=${CONCURRENCY}" --env="TOTAL=${TOTAL}" \
  -- sh -c '
    apk add --no-cache curl findutils >/dev/null 2>&1   # GNU xargs for -P
    START=$(date +%s)
    seq "$TOTAL" | xargs -P "$CONCURRENCY" -I {} \
      curl -s -o /dev/null -m 300 -w "%{http_code} %{time_total}\n" \
        "$ENDPOINT/v1/chat/completions" \
        -H "Content-Type: application/json" -d "$PAYLOAD" > /tmp/results
    END=$(date +%s)
    echo "--- HTTP status codes:"
    cut -d" " -f1 /tmp/results | sort | uniq -c
    echo "--- latency seconds (p50 / p95 / max):"
    cut -d" " -f2 /tmp/results | sort -n | awk "{a[NR]=\$1} END {print a[int(NR*0.5)], a[int(NR*0.95)], a[NR]}"
    echo "--- ${TOTAL} requests in $((END - START))s"
  '

echo "Done. Replicas scale back down about 2 minutes after load stops (downscale_delay_s)."
