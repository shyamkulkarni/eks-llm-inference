#!/usr/bin/env bash
# Smoke test: send one chat completion to the OpenAI-compatible endpoint.
# Run `kubectl port-forward svc/vllm-llm-serve-svc 8000:8000` in another terminal first.
set -euo pipefail

ENDPOINT="${1:-http://localhost:8000}"

echo "Querying ${ENDPOINT}/v1/chat/completions ..."
curl -sS "${ENDPOINT}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-1.5B-Instruct",
    "messages": [
      {"role": "user", "content": "In one sentence, what is Amazon EKS?"}
    ],
    "max_tokens": 64
  }' | python3 -m json.tool
