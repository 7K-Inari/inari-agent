# Changelog

## [0.2.0](https://github.com/7K-Inari/inari-agent/compare/v0.1.0...v0.2.0) (2026-08-14)


### Features

* agent lifecycle interfaces (Registrar, StreamClient, CapabilityWatcher, CommandHandler) ([7707c90](https://github.com/7K-Inari/inari-agent/commit/7707c90442de4822f9ed580f80dfc0fcb35be1da))
* **capability:** discovery watchers + brownfield classification ([fd27417](https://github.com/7K-Inari/inari-agent/commit/fd274175c4e418ffdb21ff58dbe5a01304a9acab))
* **cmd:** wire registrar/stream/watchers/dispatcher lifecycle ([d53a1fa](https://github.com/7K-Inari/inari-agent/commit/d53a1fad4aed11da1fbae1df885c5f5c98c352ea))
* **command:** idempotent command handler skeletons ([b7dc4f5](https://github.com/7K-Inari/inari-agent/commit/b7dc4f5fd155709c7b4ea0782a25b8f2882eeb5e))
* deploy manifests and Helm chart with minimal RBAC ([eefa88f](https://github.com/7K-Inari/inari-agent/commit/eefa88f421d568ec8139580921d87cb065506b65))
* inari-agent M0 scaffolding — manager, interfaces, deploy artifacts, CI ([bf4bb27](https://github.com/7K-Inari/inari-agent/commit/bf4bb2702b5cf0b40330e1a81df678c0da22c379))
* M1 agent connect — registrar, event stream, capability watchers, release pipeline ([1fc043a](https://github.com/7K-Inari/inari-agent/commit/1fc043a904efe889cafb39f95e1737c453186ae9))
* no-op reconcile loop and manager wiring ([afaf3e9](https://github.com/7K-Inari/inari-agent/commit/afaf3e98110e038ba218be8be9d6206130cdb744))
* **registration:** bootstrap token exchange + ESO secret resolution ([a7d03a1](https://github.com/7K-Inari/inari-agent/commit/a7d03a1d8802d466c1c9bff6bf3fb99ae281acd6))
* **stream:** EventStream client with JWT auth, keepalive, backoff, resync ([0c09463](https://github.com/7K-Inari/inari-agent/commit/0c094630af9958a3c3004a07afe41d9abae4e968))


### Bug Fixes

* **ci:** drop unused type, make fake stream connect deterministically, await secret wiring ([5781f70](https://github.com/7K-Inari/inari-agent/commit/5781f702be8e9d27a3bcbba2822287add5d35277))
* **cmd:** scope Helm discovery to INARI_HELM_NAMESPACES (least privilege) ([4f6e7cb](https://github.com/7K-Inari/inari-agent/commit/4f6e7cbb2fca9d967f61454a274b5f400d2e06b6))
* make CI lint and footprint guard actually work ([80bb9ce](https://github.com/7K-Inari/inari-agent/commit/80bb9ce24f812c98b1cc5b48f95edddfe71e5718))
* qa findings — fail-closed gate wiring, ignore exclusion, command backpressure, token caching, backoff jitter ([d11867e](https://github.com/7K-Inari/inari-agent/commit/d11867e96bf405314399c0df33e845c7a332b201))
* **release:** resolve env-context and module-path bugs in release pipeline ([d38ee31](https://github.com/7K-Inari/inari-agent/commit/d38ee31da96c618003af6cd276e34f9bf3e71a24))

## [0.1.0]

### Features

* Initial tenant-cluster agent: registration/bootstrap, capability discovery watches, gRPC client, ArgoCD command proxy (plan §5.3).
