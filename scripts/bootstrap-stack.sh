#!/usr/bin/env bash
# Bootstrap the full backing stack on the active Minikube:
#   1. Apply backing YAMLs (vLLM, Qdrant, embedding-server)
#   2. Build the Whisper image into Minikube's docker daemon
#   3. Apply whisper-server.yaml
#
# Prerequisites:
#   - Minikube running with GPU passthrough
#   - hf-token Secret exists in `default` namespace
#   - $HF_TOKEN set in your shell (used to build the Whisper image)
#   - CFM/CSM/CSA operators installed in their namespaces
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -z "${HF_TOKEN:-}" ]; then
  echo "ERROR: \$HF_TOKEN must be set (used by the Whisper Docker build)." >&2
  exit 1
fi

echo "==> Applying backing YAMLs"
kubectl apply -f k8s/backing/

echo "==> Building Whisper image into Minikube docker daemon"
eval "$(minikube docker-env)"
docker build -t streamwhisper:latest --build-arg HF_TOKEN="$HF_TOKEN" -f whisper/Dockerfile.whisper whisper/

echo "==> Applying whisper-server.yaml"
kubectl apply -f whisper/whisper-server.yaml

echo
echo "Done. NiFi flow JSONs in flows/ must be imported into the NiFi UI manually."
