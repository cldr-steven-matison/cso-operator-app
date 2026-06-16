#!/usr/bin/env bash
# Add (or replace) the `external` listener on the Strimzi Kafka CR so the
# Mac/Windows host can reach the cluster brokers via per-broker localhost
# port-forwards. Idempotent.
#
# Pair with the four port-forwards in scripts/mac-dev.sh:
#   bootstrap → localhost:19090
#   broker 0  → localhost:19094
#   broker 1  → localhost:19095
#   broker 2  → localhost:19096
set -euo pipefail

CLUSTER=${CLUSTER:-my-cluster}
NAMESPACE=${NAMESPACE:-cld-streaming}

LISTENER='{
  "name": "external",
  "port": 9094,
  "tls": false,
  "type": "loadbalancer",
  "configuration": {
    "brokers": [
      {"broker": 0, "advertisedHost": "localhost", "advertisedPort": 19094},
      {"broker": 1, "advertisedHost": "localhost", "advertisedPort": 19095},
      {"broker": 2, "advertisedHost": "localhost", "advertisedPort": 19096}
    ]
  }
}'

IDX=$(kubectl get kafka "$CLUSTER" -n "$NAMESPACE" -o json | python3 -c "
import json, sys
listeners = json.load(sys.stdin)['spec']['kafka']['listeners']
for i, l in enumerate(listeners):
    if l['name'] == 'external':
        print(i); break
")

if [ -n "$IDX" ]; then
  echo "Replacing existing external listener at index $IDX..."
  kubectl patch kafka "$CLUSTER" -n "$NAMESPACE" --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/kafka/listeners/$IDX\",\"value\":$LISTENER}]"
else
  echo "Appending new external listener..."
  kubectl patch kafka "$CLUSTER" -n "$NAMESPACE" --type=json \
    -p "[{\"op\":\"add\",\"path\":\"/spec/kafka/listeners/-\",\"value\":$LISTENER}]"
fi

echo
echo "Strimzi will roll the brokers — run the port-forwards from scripts/mac-dev.sh."
echo "When the rolling restart completes, all three brokers will advertise localhost:1909{4,5,6}."
