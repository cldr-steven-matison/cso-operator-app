#!/usr/bin/env bash
# Mac dev: port-forward backing services, then run backend + frontend.
# Stop everything with Ctrl-C (kills all spawned port-forwards).
set -euo pipefail

echo "Starting kubectl port-forwards..."
kubectl port-forward svc/vllm-service 8000:8000 &
PF_VLLM=$!
kubectl port-forward svc/qdrant 6333:6333 &
PF_QDRANT=$!
kubectl port-forward svc/embedding-server-service 8080:80 &
PF_EMBED=$!
kubectl port-forward svc/whisper-service 8001:8001 &
PF_WHISPER=$!

cleanup() {
  echo
  echo "Stopping port-forwards..."
  kill $PF_VLLM $PF_QDRANT $PF_EMBED $PF_WHISPER 2>/dev/null || true
}
trap cleanup EXIT

echo
echo "Port-forwards up. Now in two more terminals run:"
echo "  make backend"
echo "  make frontend"
echo
echo "Press Ctrl-C to stop port-forwards."
wait
