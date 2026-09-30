package instance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// IdentityKey returns the internal source identity; InstanceId remains wire-compatible.
func IdentityKey(ins *Instance) string {
	if ins == nil {
		return ""
	}
	if ins.SourceKey != "" {
		return ins.SourceKey
	}
	if ins.Label != nil && ins.Label["sourceKey"] != "" {
		return ins.Label["sourceKey"]
	}
	if ins.Provider == "" {
		return ins.InstanceId
	}
	return ins.Provider + ":" + ins.InstanceId
}

// SanitizeWireScope applies the Nacos-safe cluster scope normalization shared
// by providers and sinks. Punctuation that would interfere with the composite
// wire identity is represented by a hyphen, preserving the existing behavior.
func SanitizeWireScope(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// ConsulSourceID resolves a stable logical catalog ID. Explicit IDs must be
// wire-safe rather than silently sanitized: raw source scopes and Nacos wire
// scopes must remain identical for reconciliation and confirmed-empty prune.
func ConsulSourceID(configured string, addresses []string) (string, error) {
	configured = strings.TrimSpace(configured)
	if configured != "" {
		for _, r := range configured {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
				return "", fmt.Errorf("consul source ID %q must contain only letters, digits, hyphen, underscore, or dot", configured)
			}
		}
		if SanitizeWireScope(configured) != configured {
			return "", fmt.Errorf("consul source ID %q must be identical to its Nacos wire scope", configured)
		}
		return configured, nil
	}
	canonical := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if address != "" {
			if _, exists := seen[address]; !exists {
				seen[address] = struct{}{}
				canonical = append(canonical, address)
			}
		}
	}
	sort.Strings(canonical)
	digest := sha256.Sum256([]byte(strings.Join(canonical, ",")))
	return "consul-" + hex.EncodeToString(digest[:6]), nil
}
