#!/usr/bin/env bash
# Create the local kind cluster for rust-kvs, if it does not already exist.
#
# Safe to re-run: an existing cluster is left alone. To start over, run
#   kind delete cluster --name kvs
# and then this script again.
set -euo pipefail

CLUSTER="${CLUSTER:-kvs}"

if ! command -v kind >/dev/null 2>&1; then
    echo "kind is not installed. Try: brew install kind" >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "The Docker daemon is not running. Start Docker Desktop first." >&2
    exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "Cluster '$CLUSTER' already exists."
else
    kind create cluster --name "$CLUSTER"
fi

kubectl cluster-info --context "kind-$CLUSTER"
