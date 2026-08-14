#!/usr/bin/env bash
# Regression tests for the footprint budget guard (hack/check-footprint.sh).
#
# Usage: hack/check-footprint_test.sh
# Exercises the budget edge cases: at-budget pass, over-budget fail, missing
# limits fail, quoted/decimal cpu units parsed without crashing.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="$HERE/check-footprint.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0

check() { # name expected_exit expected_substr manifest
  local name="$1" want="$2" substr="$3" file="$4" out got
  set +e
  out="$("$GUARD" --manifest "$file" 2>&1)"
  got=$?
  set -e
  local ok=1
  if [[ "$got" != "$want" ]]; then
    echo "FAIL: $name (exit: want $want, got $got)"
    ok=0
  fi
  if [[ -n "$substr" ]] && ! grep -qF "$substr" <<<"$out"; then
    echo "FAIL: $name (output missing: $substr)"
    ok=0
  fi
  if (( ok )); then
    echo "PASS: $name"
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
  fi
}

cat > "$TMP/at_budget.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: manager
          resources:
            limits:
              cpu: 100m
              memory: 128Mi
EOF

cat > "$TMP/over_budget.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: manager
          resources:
            limits:
              cpu: 200m
              memory: 256Mi
EOF

cat > "$TMP/no_limits.yaml" <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: inari-system
EOF

cat > "$TMP/quoted_cpu.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: manager
          resources:
            limits:
              cpu: "1"
              memory: 128Mi
EOF

cat > "$TMP/decimal_cpu.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: manager
          resources:
            limits:
              cpu: 0.5
              memory: 1Gi
EOF

check "at-budget limits pass"           0 "footprint guard OK" "$TMP/at_budget.yaml"
check "over-budget limits fail"         1 "exceeds budget"     "$TMP/over_budget.yaml"
check "missing limits fail"             1 "no resource limits" "$TMP/no_limits.yaml"
check "quoted cpu parsed (not crash)"   1 "exceeds budget"     "$TMP/quoted_cpu.yaml"
check "decimal cpu parsed (not crash)"  1 "exceeds budget"     "$TMP/decimal_cpu.yaml"

if (( fail > 0 )); then
  echo "footprint tests FAILED ($fail failing, $pass passing)" >&2
  exit 1
fi
echo "footprint tests OK ($pass passing)"
