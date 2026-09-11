#!/usr/bin/env bash
# Rebuild the image, load it into the kind cluster, and restart the pod so the
# new build is actually running.
#
# All three steps matter. Loading without restarting leaves the old pod running
# the old image; restarting without loading redeploys the image the node already
# has. Either one on its own looks like a deploy that silently did nothing.
set -euo pipefail

CLUSTER="${CLUSTER:-kvs}"
IMAGE="${IMAGE:-kvs-server:dev}"
NAMESPACE="${NAMESPACE:-kvs}"
STATEFULSET="${STATEFULSET:-kvs}"

# The build context must be the repo root, so this works from any directory.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Building $IMAGE"
docker build -t "$IMAGE" "$REPO_ROOT"

echo "==> Loading $IMAGE into cluster '$CLUSTER'"
kind load docker-image "$IMAGE" --name "$CLUSTER"

if kubectl get statefulset "$STATEFULSET" -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "==> Restarting statefulset/$STATEFULSET"
    kubectl rollout restart "statefulset/$STATEFULSET" -n "$NAMESPACE"
    kubectl rollout status "statefulset/$STATEFULSET" -n "$NAMESPACE" --timeout=120s
else
    echo "==> No statefulset/$STATEFULSET in namespace '$NAMESPACE' yet."
    echo "    Apply deploy/k8s/ when Task 4 is done."
fi
