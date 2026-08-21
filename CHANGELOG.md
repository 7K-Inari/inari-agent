# Changelog

## [0.3.0](https://github.com/7K-Inari/inari-agent/compare/v0.2.0...v0.3.0) (2026-08-21)


### Features

* **argocd:** bundle/BYO lifecycle with skew policy, Application/AppProject management, register handler ([d0ef32e](https://github.com/7K-Inari/inari-agent/commit/d0ef32eb55b7cae52ed0ff1f975977b298229a05))
* **argocd:** invoke-action handler tunneling sync/refresh/rollback ([13d536c](https://github.com/7K-Inari/inari-agent/commit/13d536c7a836b824f967228918488d81cf78412c))
* **command:** per-kind handler registry, durable journal, git-backed render/apply handlers ([5fd162e](https://github.com/7K-Inari/inari-agent/commit/5fd162e0bf5225287bacaf81508e67148cf3a8af))
* **git:** provider abstraction with GitHub App auth via ESO credentials ([18a5fbb](https://github.com/7K-Inari/inari-agent/commit/18a5fbb4bc95d07f47fc996cdfaf5b2c73801795))
* M2 command handlers — git renderer, ArgoCD integration, status streamer, invoke-action ([256d954](https://github.com/7K-Inari/inari-agent/commit/256d95414bbd70dd4f716ea3a816afa26c81da85))
* **render:** KRO RGD instance renderer and OCI/git bundle fetcher ([10e50f1](https://github.com/7K-Inari/inari-agent/commit/10e50f1ffb8fc81e7c6300f90da34fa9b955638c))
* **status:** status streamer plus lifecycle wiring, RBAC, and env config ([5d89336](https://github.com/7K-Inari/inari-agent/commit/5d893365e758fbc8b0f40263a5e761995a2c2d16))


### Bug Fixes

* address M2 review findings in streamer, journal, invoke-action, OpenPR ([8be3587](https://github.com/7K-Inari/inari-agent/commit/8be3587e2f3a781f707f74d2d79deecbc4b748c5))
* **ci:** restore the verify job in the release workflow ([488e55e](https://github.com/7K-Inari/inari-agent/commit/488e55ef1de072117d11c3012e97483c8a3114e5))
* **ci:** restore the verify job in the release workflow ([73fc0df](https://github.com/7K-Inari/inari-agent/commit/73fc0df393deddbc9f00ea4d40b2c0e0b7361012))
* **ci:** strip binary debug info for image budget; fix lint findings ([20d7f58](https://github.com/7K-Inari/inari-agent/commit/20d7f58a2b74c61360c23546f059dee9a9c58690))
* **release:** write multiline image tags output with heredoc delimiter ([0169462](https://github.com/7K-Inari/inari-agent/commit/016946218be782f55b89f133c66b7be2047e47a4))
* **release:** write multiline image tags output with heredoc delimiter ([57d3748](https://github.com/7K-Inari/inari-agent/commit/57d374856479f16dbe54b4204c80527670e669ee))
* survive restarts after registration; log stream session errors ([bef4b1c](https://github.com/7K-Inari/inari-agent/commit/bef4b1c1765d09bcb0809688899e593d53aa7ac8))
* survive restarts after registration; log stream session errors ([cc77255](https://github.com/7K-Inari/inari-agent/commit/cc77255e800513299ed9cb54c59e2ff966bf1b08))
* validate file paths against path traversal; test OpenPR ([2f8676a](https://github.com/7K-Inari/inari-agent/commit/2f8676a1e0ba7a0d16443160173332cae6b4d230))

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
