IMG ?= ghcr.io/7k-inari/inari-agent:latest
CONTAINER_TOOL ?= docker
KUSTOMIZE ?= kustomize

.PHONY: build
build:
	go build -o bin/manager ./cmd

.PHONY: test
test:
	go test ./... -coverprofile cover.out

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: manifests
manifests: ## Render the single install manifest to dist/install.yaml
	mkdir -p dist
	$(KUSTOMIZE) build config/default > dist/install.yaml

.PHONY: docker-build
docker-build:
	$(CONTAINER_TOOL) build -t ${IMG} .

# Footprint budget guard (plan §5.3, §12.1/4):
#   - manifest container limits must not exceed 100m CPU / 128Mi memory
#   - image size must not exceed 50MB (distroless static baseline is ~20-30MB;
#     raise only with a documented reason in this Makefile)
.PHONY: footprint-guard
footprint-guard:
	hack/check-footprint.sh --image ${IMG}

.PHONY: tidy
tidy:
	go mod tidy
