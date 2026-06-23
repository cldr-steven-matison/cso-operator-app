#!/usr/bin/env bash
# Mac dev: port-forward backing services + Kafka external listener,
# then run backend + frontend in two more terminals.
# Stop everything with Ctrl-C (kills all spawned port-forwards).
#
# Stack-agnostic: forwards the canonical Service names (`vllm-service`,
# `whisper-service`) which the bootstrap script aliases to whichever
# stack is active (GPU or CPU). The backend's .env.local URLs are
# identical on both paths.
set -euo pipefail

STACK="${STACK:-gpu}"
echo "Stack: $STACK (informational — canonical Services route to the active backend)"
echo "Starting kubectl port-forwards..."

# Backing AI stack (default namespace)
kubectl port-forward svc/vllm-service 8000:8000 &                              ; PF_VLLM=$!
kubectl port-forward svc/qdrant 6333:6333 &                                    ; PF_QDRANT=$!
kubectl port-forward svc/embedding-server-service 8080:80 &                    ; PF_EMBED=$!
kubectl port-forward svc/whisper-service 8001:8001 &                           ; PF_WHISPER=$!

# Kafka external listener (cld-streaming namespace) — bootstrap + per-broker
# advertised hosts. Requires scripts/kafka-external-listener.sh to have been
# applied to my-cluster.
kubectl port-forward -n cld-streaming svc/my-cluster-kafka-external-bootstrap 19090:9094 &  ; PF_KBS=$!
kubectl port-forward -n cld-streaming svc/my-cluster-combined-0 19094:9094 &                ; PF_KB0=$!
kubectl port-forward -n cld-streaming svc/my-cluster-combined-1 19095:9094 &                ; PF_KB1=$!
kubectl port-forward -n cld-streaming svc/my-cluster-combined-2 19096:9094 &                ; PF_KB2=$!

cleanup() {
  echo
  echo "Stopping port-forwards..."
  kill $PF_VLLM $PF_QDRANT $PF_EMBED $PF_WHISPER \
       $PF_KBS $PF_KB0 $PF_KB1 $PF_KB2 2>/dev/null || true
}
trap cleanup EXIT

echo
echo "Port-forwards up. Now in two more terminals:"
echo "  make backend"
echo "  make frontend"
echo
echo "Press Ctrl-C to stop port-forwards."
wait
