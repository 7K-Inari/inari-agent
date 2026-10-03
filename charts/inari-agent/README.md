# inari-agent

Inari tenant-cluster agent (plan §5.3): registration/bootstrap, capability
discovery watches, outbound ConnectRPC event stream to the control plane,
GitOps renderer, ArgoCD command proxy, status streamer. Renders the same
install manifest as `config/default`.

## Production HA posture (active-passive)

```yaml
replicas: 2
leaderElection:
  enabled: true
```

At `replicas >= 2` the chart also renders a PodDisruptionBudget
(`pdb.minAvailable`, default 1) plus soft pod anti-affinity (hostname) and
zone topology spread, so voluntary disruptions keep one agent pod serving.
Leader election uses a `coordination.k8s.io` Lease in the agent namespace;
the required RBAC is always rendered.

**ACTIVE-PASSIVE ONLY.** Do NOT run active-active: the agent has no
session fencing and the command journal only bounds (does not eliminate)
double command execution. Server-side fencing is handled separately
(inari-server).

The single-replica default with leader election off is the supported dev
posture and is unchanged.

## Readiness

`/readyz` reflects control-plane stream connectivity: ready while connected,
tolerant of disconnects shorter than a grace period so brief reconnects
don't flap (standalone mode — no control plane configured — stays ready).
Liveness (`/healthz`) is a bare ping. Tune the grace period with
`config.readyzDisconnectGrace` (Go duration, built-in default `90s`).

## Kubectl tunnel agent (plan §8)

The chart also renders `inari-tunnel-agent`: a dedicated Deployment running
the second binary in the same image (`command: ["/inari-tunnel-agent"]`).
It dials out to inari-kubeproxy and relays proxied kubectl traffic to the
tenant apiserver using hub-minted `Impersonate-*` headers — pull, never
push.

Its ServiceAccount (`inari-tunnel-agent`) holds exactly three rules —
`impersonate` on `users`, `groups`, `uids` — and nothing else. Never merge
this into the inari-agent SA.

Set `kubectlTunnel.enabled: false` to skip all tunnel resources. The
`tunnel-<cluster-id>` client secret is consumed from the Secret named by
`kubectlTunnel.secret.name/key`; optionally project it via ESO with
`kubectlTunnel.oidcSecret.create` (mirrors the top-level `oidcSecret`
pattern). `kubectlTunnel.kubeproxyURL`, `clusterID`, and
`oidcIssuer`/`oidcTokenURL` configure the dial-out.

## Values

See `values.yaml` for the commented key reference; `values.schema.json`
validates all keys.
