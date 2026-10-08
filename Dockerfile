FROM golang:1.27@sha256:e432b43af23a9328d56a7c499be0476810aa344acbcf65fc7c455d4ff5a40602 AS builder
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

FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/inari-tunnel-agent .
USER 65532:65532

ENTRYPOINT ["/manager"]
