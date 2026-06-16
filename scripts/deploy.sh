#!/usr/bin/env bash
# Build the app image into Minikube and deploy.
set -euo pipefail

cd "$(dirname "$0")/.."

eval "$(minikube docker-env)"
docker build -t cso-operator-app:latest .

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

echo
minikube service cso-operator-app --url
