#!/usr/bin/env bash
# Build the app image into Minikube and deploy as an in-cluster pod.
# Works identically on Mac and Windows once the backing stack is up.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Building app image into Minikube docker daemon"
eval "$(minikube docker-env)"
docker build -t cso-operator-app:latest --build-arg MODULES="${MODULES:-}" .

echo "==> Applying app manifests"
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

echo "==> Restarting pod to pick up new image (imagePullPolicy: Never requires explicit restart)"
kubectl rollout restart deploy/cso-operator-app

echo "==> Waiting for rollout"
kubectl rollout status deploy/cso-operator-app --timeout=120s

echo
echo "App URL:"
minikube service cso-operator-app --url
