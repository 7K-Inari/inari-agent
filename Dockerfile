FROM golang:1.27@sha256:87f49b77a7b6a99ad7dc656be010ae453322459dd2ed902ac177649a302bc101 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -ldflags "-s -w -X main.version=${VERSION}" -o manager ./cmd && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -ldflags "-s -w -X main.version=${VERSION}" -o inari-tunnel-agent ./cmd/inari-tunnel-agent

FROM gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/inari-tunnel-agent .
USER 65532:65532

ENTRYPOINT ["/manager"]
