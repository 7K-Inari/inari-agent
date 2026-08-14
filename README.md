# inari-agent

Tenant-cluster controller for Inari: registration/bootstrap, capability discovery watches, event-stream client, GitOps renderer, ArgoCD command proxy, status streamer (plan §5.3).

Stack: Go, controller-runtime / kubebuilder

Part of the **Inari** multi-tenant Internal Developer Platform (GitHub org `7K-Inari`).
Canonical architecture & development plan: [inari-docs/docs/architecture/inari-platform-plan.md](https://github.com/7K-Inari/inari-docs/blob/main/docs/architecture/inari-platform-plan.md)

## Lifecycle (plan §5.3)

1. **Register** — the install manifest carries a one-time TTL'd registration token. The agent exchanges it (RegistrationService RPC, unauthenticated except for the token) for a per-cluster Keycloak OIDC client. The token is forgotten immediately after the exchange; the OIDC client secret is delivered in-cluster via the External Secrets Operator — it never crosses the registration API and is never committed to git.
2. **Connect** — the agent dials out to the Agent Gateway and opens the bidirectional `EventStream`, authenticating with short-lived client-credentials JWTs (`cluster_id` claim). Ping/pong keepalive with a receive dead-man switch, exponential-backoff reconnect (1s → 5m), and a handshake carrying the last state checksum so the gateway can trigger a full resync after partitions.
3. **Discover** — read-only watches on CRDs (OpenAPI v3 schemas + CEL validations), OLM CSVs (specDescriptors), Crossplane XRDs/Compositions/providers, Helm releases (label metadata only), KRO RGDs, and cluster metadata (k8s version, node labels/taints, addon versions) are streamed upstream as `capability-update` events.
4. **Reconcile** — commands (`apply-bundle`, `register-argocd-app`, `invoke-action`, `render-rgd-instance`) are accepted and acked idempotently (`command_id` is the idempotency key; duplicate deliveries replay the recorded ack). Real Git/ArgoCD/KRO execution lands in M2. Commands fail closed while disconnected.

**Brownfield default (plan §12.1/3):** every pre-existing resource is classified `observe-only` — nothing is mutated on first connect. Set the `inari.dev/management: adopt|ignore` annotation on a resource to change its classification.

## Configuration

| Env var | Purpose |
|---|---|
| `INARI_REGISTRATION_TOKEN` | One-time bootstrap token (rendered into the install manifest by the server). Absent ⇒ standalone mode (healthy, no upstream connection). |
| `INARI_TENANT_ID` | Owning tenant ID. |
| `INARI_CONTROL_PLANE` | Agent Gateway base URL, e.g. `https://gw.inari.example`. |
| `INARI_CLUSTER_LABELS` | Comma-separated `k=v` labels reported at registration (ClusterSet targeting). |
| `INARI_HELM_NAMESPACES` | Namespaces whose Helm release Secrets are discovered (default `inari-system`). Deliberately namespace-scoped: no cluster-wide Secrets read. |

## Egress-only environments / HTTP proxy (plan §12.1/5)

The agent only ever dials out; the control plane never connects in. The stream transport is HTTP/2 (ConnectRPC) and honours the standard proxy environment variables:

```yaml
env:
  - name: HTTPS_PROXY
    value: "http://proxy.corp:3128"   # HTTP/2 CONNECT through the proxy
  - name: NO_PROXY
    value: ".cluster.local,10.0.0.0/8"
```

TLS-intercepting proxies must trust the proxy CA in the agent container. A WebSocket transport fallback and air-gapped operation are deferred to v2.

## Releases

Tagging `v*.*.*` runs the release pipeline: lint/test/footprint guard → multi-arch image to `ghcr.io/7k-inari/inari-agent` (`:latest` on stable releases) → cosign keyless signature + SPDX SBOM attestation + SLSA3 provenance → install manifest attached to the GitHub Release and the Helm chart pushed to `oci://ghcr.io/7k-inari/charts`. Every merge to `main` publishes `ghcr.io/7k-inari/inari-agent:edge-<sha>` (and `:edge`).

Verify an image:

```sh
cosign verify ghcr.io/7k-inari/inari-agent:v0.1.0 \
  --certificate-identity-regexp 'https://github.com/7K-Inari/inari-agent/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Footprint budget

The agent must fit starter-tier nodes: **≤ 100m CPU / 128Mi memory** limits and an image ≤ 50MB, enforced in CI by `hack/check-footprint.sh` (plan §12.1/4).

## Development

```sh
make test            # unit tests
make lint            # golangci-lint
make manifests       # render dist/install.yaml
make footprint-guard # image-size budget check
```
