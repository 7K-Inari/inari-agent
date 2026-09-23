package capability

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"google.golang.org/protobuf/proto"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// StateChecksum computes a stable checksum over the agent's full capability
// snapshot. The gateway compares it after reconnect to decide on resync
// (plan §5.2). Deterministic and independent of snapshot ordering.
func StateChecksum(caps []*agentv1.Capability) string {
	digests := make(map[string][]byte, len(caps))
	for _, c := range caps {
		if c == nil {
			continue
		}
		sum, ok := DigestCapability(c)
		if !ok {
			continue
		}
		digests[capabilityKey(c)] = sum
	}
	return ChecksumDigests(digests)
}

// DigestCapability returns the per-capability digest used by StateChecksum.
// The Aggregator caches these so per-event checksums don't re-marshal the
// entire snapshot (O(N) per event → O(1), issue #28).
func DigestCapability(c *agentv1.Capability) ([]byte, bool) {
	// Marshal with deterministic field ordering.
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, false
	}
	sum := sha256.Sum256(raw)
	return sum[:], true
}

// ChecksumDigests combines per-capability digests (keyed by capabilityKey)
// into the stable full-state checksum.
func ChecksumDigests(digests map[string][]byte) string {
	keys := make([]string, 0, len(digests))
	for k := range digests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(digests[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func capabilityKey(c *agentv1.Capability) string {
	return c.Kind.String() + "/" + c.Group + "/" + c.Name + "/" + c.Version
}
