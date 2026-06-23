#!/usr/bin/env bash
# Build the app image into Minikube and deploy as an in-cluster pod.
# Works identically on Mac and Windows once the backing stack is up.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Building app image into Minikube docker daemon"
eval "$(minikube docker-env)"
docker build -t cso-operator-app:latest .

echo "==> Mirroring nifi-admin-creds (cfm-streaming) into default/nifi-app-creds"
# Cross-namespace Secret refs aren't supported by k8s. The app pod lives
# in `default`, so we copy the NiFi admin Secret into this namespace. The
# Deployment's env block reads `nifi-app-creds` with optional: true, so
# missing the source Secret is non-fatal — the app just won't have a
# NiFi password and NiFi calls will fail until you set one.
if kubectl get secret nifi-admin-creds -n cfm-streaming >/dev/null 2>&1; then
  kubectl get secret nifi-admin-creds -n cfm-streaming -o json \
    | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.ownerReferences)
          | .metadata.name = "nifi-app-creds"' \
    | kubectl apply -n default -f -
else
  echo "    (nifi-admin-creds not found in cfm-streaming — skipping; NiFi API calls will 401 until it exists)"
fi

echo "==> Applying app manifests"
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

echo "==> Waiting for rollout"
kubectl rollout status deploy/cso-operator-app --timeout=120s

echo
echo "App URL:"
minikube service cso-operator-app --url
