#!/usr/bin/env bash
# Output quality check: are the model's answers coherent? Run it after any
# deployment or engine-flag change.
# Sends a few fixed prompts at temperature 0 from inside the cluster and flags
# any answer that doesn't contain the expected keyword.
#
# Usage: ./scripts/quality-check.sh [ROUNDS]
set -uo pipefail

ROUNDS="${1:-3}"
CLUSTER=$(kubectl get rayservice vllm-llm -o jsonpath="{.status.activeServiceStatus.rayClusterName}")
HEAD=$(kubectl get pods -l "ray.io/cluster=${CLUSTER},ray.io/node-type=head" --no-headers -o custom-columns=":metadata.name" | head -1)

kubectl exec -i "$HEAD" -c ray-head -- env ROUNDS="$ROUNDS" PYTHONUNBUFFERED=1 python3 - <<'PY'
import json, os, urllib.request

URL = "http://vllm-llm-serve-svc:8000/v1/chat/completions"
CHECKS = [
    ("In one sentence, what is Amazon EKS?", "kubernetes"),
    ("What is the capital of France?", "paris"),
    ("Name three primary colors.", "red"),
    # No arithmetic here: this 1.5B model answers "12 times 12 is 14" at
    # temperature 0. That is a model-capability slip, not the garbage-token
    # failure this check exists to catch, so arithmetic isn't tested here.
    ("What color is the sky on a clear day?", "blue"),
]

def ask(prompt):
    body = json.dumps({
        "model": "Qwen/Qwen2.5-1.5B-Instruct",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 40, "temperature": 0,
    }).encode()
    req = urllib.request.Request(URL, data=body, headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=120))["choices"][0]["message"]["content"]

bad = total = 0
for _ in range(int(os.environ["ROUNDS"])):
    for prompt, keyword in CHECKS:
        total += 1
        answer = ask(prompt).replace("\n", " ")
        ok = keyword in answer.lower()
        bad += not ok
        print(("ok  " if ok else "BAD ") + prompt[:38].ljust(38) + " -> " + answer[:70])
print(f"\n{total - bad}/{total} coherent answers")
raise SystemExit(1 if bad else 0)
PY
