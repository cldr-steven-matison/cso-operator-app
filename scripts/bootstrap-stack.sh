#!/usr/bin/env bash
# Bootstrap the full backing stack on the active Minikube.
#
# STACK=gpu (default):
#   1. Apply backing YAMLs (vLLM GPU, Qdrant, embedding-server)
#   2. Build the GPU Whisper image into Minikube's docker daemon
#   3. Apply whisper-server.yaml
#
# STACK=cpu (Mac dev, no GPU):
#   1. Apply backing YAMLs (vLLM CPU via llama.cpp, Qdrant, embedding-server)
#   2. Apply a Service alias so `vllm-service` resolves to CPU pods
#   3. Build the CPU Whisper image (faster-whisper small int8)
#   4. Apply whisper-server-cpu.yaml + the `whisper-service` alias
#
# Prerequisites:
#   - Minikube running (GPU passthrough only required for STACK=gpu)
#   - hf-token Secret exists in `default` namespace
#   - $HF_TOKEN set in your shell (required for GPU Whisper build; optional for CPU)
#   - CFM/CSM/CSA operators installed in their namespaces
set -euo pipefail

cd "$(dirname "$0")/.."

STACK="${STACK:-gpu}"
echo "==> Stack: $STACK"

case "$STACK" in
  gpu)
    if [ -z "${HF_TOKEN:-}" ]; then
      echo "ERROR: \$HF_TOKEN must be set (used by the GPU Whisper Docker build)." >&2
      exit 1
    fi

    echo "==> Removing any CPU stack aliases (so canonical Services point at GPU pods)"
    kubectl delete -f whisper/whisper-service-cpu-alias.yaml --ignore-not-found
    kubectl delete -f k8s/backing/vllm-service-cpu-alias.yaml --ignore-not-found
    kubectl delete -f whisper/whisper-server-cpu.yaml --ignore-not-found
    kubectl delete -f k8s/backing/vllm-cpu.yaml --ignore-not-found
    kubectl delete -f embed/embed-server-cpu.yaml --ignore-not-found

    echo "==> Applying backing YAMLs (GPU)"
    kubectl apply -f k8s/backing/vllm-Qwen2.5-3B-Instruct.yaml \
                  -f k8s/backing/qdrant-deployment.yaml \
                  -f k8s/backing/embedding-server.yaml

    echo "==> Adding Kafka external listener (idempotent)"
    bash scripts/kafka-external-listener.sh

    echo "==> Building GPU Whisper image into Minikube docker daemon"
    eval "$(minikube docker-env)"
    docker build -t streamwhisper:latest \
      --build-arg HF_TOKEN="$HF_TOKEN" \
      -f whisper/Dockerfile.whisper whisper/

    echo "==> Applying whisper-server.yaml (GPU)"
    kubectl apply -f whisper/whisper-server.yaml
    ;;

  cpu)
    echo "==> Removing GPU vllm/whisper/embedding Services so CPU aliases can claim the canonical names"
    # The GPU vllm-Qwen2.5-3B-Instruct.yaml publishes Service `vllm-service`.
    # whisper-server.yaml publishes Service `whisper-service`.
    # embedding-server.yaml publishes Service `embedding-server-service`
    # (and uses the upstream TEI image, which is amd64-only — does not
    # run on Apple Silicon).
    kubectl delete service vllm-service --ignore-not-found
    kubectl delete deployment vllm-server --ignore-not-found
    kubectl delete service whisper-service --ignore-not-found
    kubectl delete deployment whisper-server --ignore-not-found
    kubectl delete service embedding-server-service --ignore-not-found
    kubectl delete deployment embedding-server --ignore-not-found

    echo "==> Applying backing YAMLs (CPU vLLM + Qdrant)"
    kubectl apply -f k8s/backing/vllm-cpu.yaml \
                  -f k8s/backing/qdrant-deployment.yaml

    echo "==> Applying vllm-service alias so canonical DNS points at CPU pods"
    kubectl apply -f k8s/backing/vllm-service-cpu-alias.yaml

    echo "==> Adding Kafka external listener (idempotent)"
    bash scripts/kafka-external-listener.sh

    eval "$(minikube docker-env)"

    echo "==> Building CPU Whisper image into Minikube docker daemon"
    docker build -t streamwhisper-cpu:latest \
      -f whisper/Dockerfile.whisper.cpu whisper/

    echo "==> Building CPU embedding-server image (arm64-native sentence-transformers)"
    docker build -t embed-server-cpu:latest \
      ${HF_TOKEN:+--build-arg HF_TOKEN="$HF_TOKEN"} \
      -f embed/Dockerfile.embed.cpu embed/

    echo "==> Applying whisper-server-cpu.yaml + whisper-service alias"
    kubectl apply -f whisper/whisper-server-cpu.yaml
    kubectl apply -f whisper/whisper-service-cpu-alias.yaml

    echo "==> Applying embed-server-cpu.yaml (Service replaces canonical embedding-server-service)"
    kubectl apply -f embed/embed-server-cpu.yaml
    ;;

  *)
    echo "ERROR: unknown STACK='$STACK' (expected 'gpu' or 'cpu')" >&2
    exit 1
    ;;
esac

echo
echo "Done. NiFi flow JSONs in flows/ must be imported into the NiFi UI manually."
