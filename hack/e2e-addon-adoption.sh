#!/usr/bin/env bash
# Live addon-adoption e2e (Feature C, risk item 2): provisions a kind
# cluster, installs ArgoCD (pinned to the bundle line) and a real
# helm-installed cert-manager, then runs the e2e-tagged Go test that
# drives scanner discovery and in-place adoption with prune OFF +
# take-ownership, asserting the existing release survives the ArgoCD sync.
#
# Prerequisites: docker, kind, kubectl, helm. Not CI-gated (nightly/manual).
set -euo pipefail

CLUSTER=${CLUSTER:-inari-addon-e2e}
ARGOCD_VERSION=${ARGOCD_VERSION:-v3.3.8}
CERT_MANAGER_VERSION=${CERT_MANAGER_VERSION:-v1.16.2}
export KUBECONFIG=${KUBECONFIG:-$HOME/.kube/kind-${CLUSTER}.yaml}

cleanup() {
  if [[ "${KEEP_CLUSTER:-}" != "1" ]]; then
    kind delete cluster --name "$CLUSTER" || true
  fi
}
trap cleanup EXIT

kind create cluster --name "$CLUSTER" --wait 120s

kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"
kubectl -n argocd rollout status deploy/argocd-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-repo-server --timeout=300s

helm repo add jetstack https://charts.jetstack.io --force-update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --version "$CERT_MANAGER_VERSION" --set crds.enabled=true
kubectl -n cert-manager rollout status deploy/cert-manager --timeout=300s

go test -tags=e2e -count=1 -timeout 15m ./internal/e2e/ -run TestLiveAddonAdoption -v
