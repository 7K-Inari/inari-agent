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
	keys := make([]string, 0, len(caps))
	digests := make(map[string][]byte, len(caps))
	for _, c := range caps {
		if c == nil {
			continue
		}
		key := capabilityKey(c)
		// Marshal with deterministic field ordering.
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(raw)
		digests[key] = sum[:]
		keys = append(keys, key)
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
