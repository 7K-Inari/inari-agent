# inari-agent — Agent Guide

Tenant-cluster controller for Inari: registration/bootstrap, capability discovery watches, gRPC client, GitOps renderer, ArgoCD command proxy, status streamer (plan §5.3).

Stack: Go, controller-runtime / kubebuilder

## Key architecture constraints
- **Outbound-only** bidirectional gRPC stream to the control plane (argocd-agent model); control plane never connects in (§5.3).
- Registration: one-time TTL'd token → per-cluster Keycloak OIDC client (client-credentials, short-lived JWT, hardcoded `cluster_id` claim); bootstrap token forgotten after exchange; client secret delivered via ESO — never in git (§5.3).
- Capability discovery watches: CRDs (OpenAPI v3 schemas), OLM CSVs, Crossplane XRDs/Compositions/providers, Helm releases, KRO RGDs, cluster metadata → `capability-update` events (§5.3).
- **Footprint budget: ≤ 100m CPU / 128Mi memory — enforced in CI** (§5.3, §12.1/4).
- Idempotent command handlers; at-least-once delivery; checksum-based resync on reconnect (§5.2 Agent Gateway, §5.3).
- Brownfield default: pre-existing resources are **observe-only** unless explicitly adopted (§5.3, §11/3).
- Agent RBAC: dedicated tenant-scoped ServiceAccount; watches read-only; mutations limited to Inari-managed namespaces/resources (§5.3).
- Protocol protos come from the `inari-api` repo — pin its versioned packages (§6).

## Conventions
- Conventional Commits; SemVer releases; container images/artifacts cosign-signed (once CI exists).
- Releases are automated via release-please in PR-only mode (see docs/release.md): `release-please.yml` opens/updates the Release PR on push to main (version bump + CHANGELOG.md, nothing else) → maintainer merges → `release.yml` creates tag `vX.Y.Z` + GitHub Release and runs publish (GHCR image + cosign/SBOM/SLSA, install manifest, Helm chart OCI). Never hand-create tags/Releases or edit the manifest. Per-commit edge images stay in `ci.yaml`.
- Write tests for new behavior; keep changes minimal and focused.
- Canonical architecture & development plan: https://github.com/7K-Inari/inari-docs/blob/main/docs/architecture/inari-platform-plan.md (section references below point into it).

## Platform design principles (apply everywhere)
1. Tenant-aware to the core — every object carries a tenant ID; every API decision is tenant-scoped.
2. Zero tenant credentials on the hub — no tenant kubeconfigs or cloud keys in the control plane.
3. Pull, never push — agents dial out; the control plane never initiates connections into tenant networks.
4. Desired state, eventually reconciled — GitOps/CR-based mutations, not imperative RPCs.
5. The catalog is a projection of reality — capabilities are discovered, not declared.
6. Small kernel, everything else extension.
7. Modular monolith first — strict internal module boundaries.
