#!/usr/bin/env bash
# Footprint budget guard (plan §5.3, §12.1/4).
#
# Budgets:
#   - Deployment container limits MUST NOT exceed 100m CPU / 128Mi memory.
#   - Agent image MUST NOT exceed 50MB as reported by `docker image inspect
#     .Size` (sum of uncompressed layers; the distroless static Go baseline is
#     ~20-30MB; raise only with a documented reason in Makefile).
#
# Usage:
#   hack/check-footprint.sh [--manifest FILE] [--image IMAGE:TAG]
#
# --manifest defaults to dist/install.yaml (run `make manifests` first).
# --image triggers a `docker image inspect` size check (requires the image
# to be present locally).
set -euo pipefail

MANIFEST="dist/install.yaml"
IMAGE=""
MAX_CPU_MILLICORES=100
MAX_MEMORY_MIB=128
MAX_IMAGE_MB=50

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) MANIFEST="$2"; shift 2 ;;
    --image)    IMAGE="$2";    shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

to_millicores() { # "100m" -> 100, "1" -> 1000
  local v="$1"
  if [[ "$v" == *m ]]; then echo "${v%m}"; else echo "$((v * 1000))"; fi
}

to_mib() { # "128Mi" -> 128, "1Gi" -> 1024
  local v="$1"
  case "$v" in
    *Mi) echo "${v%Mi}" ;;
    *Gi) echo "$((${v%Gi} * 1024))" ;;
    *) echo "unsupported memory unit: $v" >&2; exit 1 ;;
  esac
}

echo "==> Checking manifest limits in ${MANIFEST}"
[[ -f "$MANIFEST" ]] || { echo "manifest not found: $MANIFEST (run: make manifests)" >&2; exit 1; }

fail=0
while IFS=$'\t' read -r cpu mem; do
  cpu_mc=$(to_millicores "$cpu")
  mem_mb=$(to_mib "$mem")
  echo "    found limits: cpu=${cpu} (${cpu_mc}m), memory=${mem} (${mem_mb}Mi)"
  if (( cpu_mc > MAX_CPU_MILLICORES )); then
    echo "FAIL: cpu limit ${cpu} exceeds budget ${MAX_CPU_MILLICORES}m" >&2; fail=1
  fi
  if (( mem_mb > MAX_MEMORY_MIB )); then
    echo "FAIL: memory limit ${mem} exceeds budget ${MAX_MEMORY_MIB}Mi" >&2; fail=1
  fi
done < <(awk '
  /limits:/ { in_limits=1; cpu=""; mem=""; next }
  in_limits && /cpu:/ { gsub(/[[:space:]]|cpu:/, ""); cpu=$0 }
  in_limits && /memory:/ { gsub(/[[:space:]]|memory:/, ""); mem=$0; print cpu "\t" mem; in_limits=0 }
' "$MANIFEST")

if [[ -n "$IMAGE" ]]; then
  echo "==> Checking image size for ${IMAGE}"
  size_bytes=$(docker image inspect "$IMAGE" --format '{{.Size}}')
  size_mb=$(( size_bytes / 1024 / 1024 ))
  echo "    image size: ${size_mb}MB (budget ${MAX_IMAGE_MB}MB)"
  if (( size_mb > MAX_IMAGE_MB )); then
    echo "FAIL: image size ${size_mb}MB exceeds budget ${MAX_IMAGE_MB}MB" >&2; fail=1
  fi
fi

if (( fail )); then
  echo "footprint guard FAILED" >&2
  exit 1
fi
echo "footprint guard OK"
