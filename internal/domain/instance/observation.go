package instance

import "strings"

// PodObservation is the infrastructure-neutral subset of pod data used by
// domain state, status, and environment policies.
type PodObservation struct {
	Phase                 string
	PodReason             string
	WaitingReason         string
	LastTerminationReason string
	ContainerReady        bool
	ContainerRunning      bool
	Deleting              bool
	Labels                map[string]string
	Env                   map[string]string
	Event                 string
}

// StateOf maps an observation to the provider-independent instance state.
func StateOf(obs PodObservation) string {
	if obs.Deleting {
		return InstanceStateTerminated
	}

	switch obs.Phase {
	case "Pending":
		return InstanceStatePending
	case "Unknown":
		return InstanceStateUnknown
	case "Failed":
		if obs.PodReason == "Evicted" {
			return InstanceStateEvicted
		}
		return InstanceStateFailed
	case "Succeeded":
		return InstanceStateTerminated
	case "Running":
		if obs.ContainerReady {
			return InstanceStateRunning
		}
		if obs.WaitingReason == "CrashLoopBackOff" {
			if obs.LastTerminationReason == "Error" {
				return InstanceStateError
			}
			return InstanceStateCrash
		}
		return InstanceStateProbing
	default:
		return InstanceStateUnknown
	}
}

// StatusOf maps an observation to the provider-independent instance status.
func StatusOf(obs PodObservation) int32 {
	if obs.Deleting || obs.Event == "delete" {
		return InstanceStatusOffline
	}

	switch obs.Phase {
	case "Pending":
		return InstanceStatusUnhealthy
	case "Unknown":
		return InstanceStatusOffline
	case "Failed":
		if obs.PodReason == "Evicted" {
			return InstanceStatusUnhealthy
		}
		return InstanceStatusOffline
	case "Succeeded":
		return InstanceStatusOffline
	case "Running":
		if obs.ContainerReady && obs.ContainerRunning {
			return InstanceStatusOnline
		}
		return InstanceStatusUnhealthy
	default:
		return InstanceStatusOffline
	}
}

// EnvTypeOf resolves the environment type from discovered data and labels.
func EnvTypeOf(obs PodObservation, discovered string) string {
	envType := discovered
	if value := obs.Labels["env-type"]; value != "" && discovered != "" {
		envType = value
	} else if discovered == "" {
		envType = obs.Labels["K8S_CLUSTER_TYPE"]
	}
	envType = strings.ToLower(envType)
	if envType == "online" {
		return EnvProduct
	}
	return envType
}
