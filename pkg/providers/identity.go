package providers

import (
	"spotter/internal/domain/instance"
	sv "spotter/pkg/beehive/service/v2"
)

// IdentityIndex indexes instances and reports duplicate internal identities.
func IdentityIndex(items []*sv.Instance) (map[string]*sv.Instance, []string) {
	idx := make(map[string]*sv.Instance)
	seen := make(map[string]bool)
	var ambiguous []string
	for _, item := range items {
		if item == nil {
			continue
		}
		key := IdentityKey(item)
		if _, exists := idx[key]; exists && !seen[key] {
			ambiguous = append(ambiguous, key)
			seen[key] = true
			continue
		}
		if !seen[key] {
			idx[key] = item
		}
	}
	return idx, ambiguous
}

// IdentityKey returns an internal source identity while preserving wire
// InstanceId compatibility. K8s writes sourceKey into Label metadata.
func IdentityKey(ins *sv.Instance) string {
	return instance.IdentityKey(ins)
}
