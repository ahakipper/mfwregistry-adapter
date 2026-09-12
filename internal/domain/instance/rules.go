package instance

import (
	"errors"
	"fmt"
	"time"
)

const (
	InstanceStatePending    = "pending"
	InstanceStateStarting   = "starting"
	InstanceStateProbing    = "probing"
	InstanceStateOOM        = "oom"
	InstanceStateCrash      = "crash"
	InstanceStateRunning    = "running"
	InstanceStateError      = "error"
	InstanceStateFailed     = "failed"
	InstanceStateTerminated = "terminated"
	InstanceStateEvicted    = "evicted"
	InstanceStateUnknown    = "unknown"
)

const (
	InstanceStatusUnknown   int32 = 0
	InstanceStatusOnline    int32 = 1
	InstanceStatusUnhealthy int32 = 2
	InstanceStatusOffline   int32 = 3
)

const (
	ProtoHTTP      = "http"
	ProtoGRPC      = "grpc"
	ProtoWebSocket = "websocket"
	ProtoDubbo     = "dubbo"
)

const (
	ProviderK8s = "k8s"
	ProviderEcs = "ecs"
)

const (
	EnvDev     = "dev"
	EnvTest    = "test"
	EnvStaging = "staging"
	EnvProduct = "product"
)

const (
	InstanceCompatibilityLabelAosNamespace = "compatibility:aos_namespace"
	InstanceCompatibilityLabelAosApp       = "compatibility:aos_app"
	InstanceCompatibilityLabelAosDrHost    = "compatibility:aos_dr_host"
	InstanceCompatibilityLabelAosMark      = "compatibility:aos_mark"
	InstanceCompatibilityLabelEnvSan       = "env:san"
	InstanceSpringApplicationName          = "spring.application.name"
)

const FullPushInterval = 21600 * time.Second

// InstanceFilter validates an instance before it is synchronized.
type InstanceFilter func(ins *Instance) error

// DiffPolicy classifies a provider instance against the remote instance with the same ID.
type DiffPolicy func(old, new *Instance) bool

// ListToMap indexes instances by ID. Later entries replace earlier entries.
func ListToMap(ins []*Instance) map[string]*Instance {
	m := make(map[string]*Instance)
	for _, value := range ins {
		if value != nil {
			m[IdentityKey(value)] = value
		}
	}
	return m
}

// InitInstanceFilters returns the default validation filter.
func InitInstanceFilters() []InstanceFilter {
	return []InstanceFilter{
		func(ins *Instance) error {
			if ins == nil {
				return errors.New("nil resource instance")
			}
			if ins.AppCode == "" {
				return errors.New("instance has nil appcode")
			}
			if ins.EnvType == "" {
				return errors.New("instance has nil env type")
			}
			if ins.Status == InstanceStatusOnline && ins.Ip == "" {
				return errors.New("instance has nil ip when it on online status")
			}
			if ins.State == InstanceStatePending {
				return errors.New("instance has nil ip when it on heal check status and pending state")
			}
			if ins.Reversion == 0 {
				return errors.New("instance has nil reversion")
			}
			if ins.Status == InstanceStatusUnknown {
				return fmt.Errorf("instance has status unknown value: %d, may be the format process need to be performed", ins.Status)
			}
			return nil
		},
	}
}

// DiffNewerReversion is the Kubernetes-compatible policy: newer revisions win,
// while the selected fields are compared when revisions are not newer.
func DiffNewerReversion(old, new *Instance) bool {
	if old == nil || new == nil {
		return false
	}
	if old.Status == new.Status && old.Status == InstanceStatusOffline {
		return false
	}
	if new.Reversion > old.Reversion {
		return true
	}
	return new.EnvType != old.EnvType ||
		new.State != old.State ||
		new.Status != old.Status ||
		new.EnvGroup != old.EnvGroup ||
		new.InstanceId != old.InstanceId ||
		new.Ip != old.Ip
}

// DiffEqualReversion compares the fields used by the equal-revision policy.
func DiffEqualReversion(old, new *Instance) bool {
	if old == nil || new == nil || old.Reversion != new.Reversion {
		return false
	}
	return old.EnvType != new.EnvType ||
		old.EnvGroup != new.EnvGroup ||
		old.Status != new.Status ||
		old.State != new.State ||
		old.Ip != new.Ip ||
		old.Idc != new.Idc ||
		old.Cluster != new.Cluster ||
		old.Enabled != new.Enabled ||
		old.AppCode != new.AppCode ||
		old.Cpu != new.Cpu
}

// CompareThreeWay classifies instances by ID while preserving provider and remote input order.
// The provider instance is returned for same-ID differences selected by diff.
func CompareThreeWay(provider, remote []*Instance, diff DiffPolicy) (providerOnly, remoteOnly, changed []*Instance) {
	remoteByID := ListToMap(remote)
	providerByID := ListToMap(provider)
	// Legacy wire snapshots may omit SourceKey/Provider. Permit a fallback by
	// InstanceId only when that wire id is unique on both sides; duplicates are
	// quarantined to avoid cross-cluster false matches.
	legacyRemote := uniqueWireIDs(remote)
	legacyProvider := uniqueWireIDs(provider)

	for _, current := range provider {
		if current == nil {
			continue
		}
		other, ok := remoteByID[IdentityKey(current)]
		if !ok && legacyProvider[current.InstanceId] == current && legacyRemote[current.InstanceId] != nil {
			candidate := legacyRemote[current.InstanceId]
			if isLegacy(current) || isLegacy(candidate) {
				other, ok = candidate, true
			}
		}
		if !ok {
			providerOnly = append(providerOnly, current)
			continue
		}
		if diff != nil && diff(other, current) {
			changed = append(changed, current)
		}
	}
	for _, current := range remote {
		if current == nil {
			continue
		}
		if _, ok := providerByID[IdentityKey(current)]; !ok && !(legacyRemote[current.InstanceId] == current && legacyProvider[current.InstanceId] != nil && (isLegacy(current) || isLegacy(legacyProvider[current.InstanceId]))) {
			remoteOnly = append(remoteOnly, current)
		}
	}
	return providerOnly, remoteOnly, changed
}

func isLegacy(item *Instance) bool {
	return item == nil || (item.SourceKey == "" && item.SourceCluster == "" && (item.Label == nil || (item.Label["sourceKey"] == "" && item.Label["sourceCluster"] == "")))
}

func uniqueWireIDs(items []*Instance) map[string]*Instance {
	result := make(map[string]*Instance)
	duplicates := make(map[string]bool)
	for _, item := range items {
		if item == nil || item.InstanceId == "" || duplicates[item.InstanceId] {
			continue
		}
		if _, exists := result[item.InstanceId]; exists {
			delete(result, item.InstanceId)
			duplicates[item.InstanceId] = true
			continue
		}
		result[item.InstanceId] = item
	}
	return result
}

// ComposeEnvCode builds the wire-format environment code.
func ComposeEnvCode(envType, envGroup string) string {
	return envType + "#" + envGroup
}
