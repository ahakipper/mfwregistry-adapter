package nacos

import (
	"fmt"
	"strconv"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

// SinkName is the fanout sink name of the Nacos adapter: the value the
// server wiring registers it under, the retry queue keys its per-sink
// entries by, and the sync_error_gauge label uses. Exported from pkg/nacos
// (not pkg/worker) because the adapter owns its own name; the wiring site
// is its only consumer.
const SinkName = "nacos"

// Sink is the Nacos ports.InstanceSink: it maps domain instances onto the
// Nacos v1 OpenAPI (plan §7.3) and owns the PushAll prune reconcile
// (plan §7.4).
//
// Instances are registered PERSISTENT (ephemeral=false): spotter is a
// replicator asserting the desired state of other people's instances, not a
// service instance — an ephemeral registration would mass-expire live
// instances on a spotter hiccup, the opposite of the sink's contract. The
// cost (drift persists if spotter dies) is bounded by the retry queue and
// the full-push prune.
type Sink struct {
	client *Client
	logger ports.Logger
}

// Sink satisfies the internal/ports.InstanceSink port exactly (the
// compile-time check fails on any future signature drift).
var _ ports.InstanceSink = (*Sink)(nil)

// NewSink creates a Nacos sink bound to addr. A nil logger is defaulted.
func NewSink(addr string, logger ports.Logger) (*Sink, error) {
	client, err := NewClient(addr, logger)
	if err != nil {
		return nil, err
	}
	return &Sink{client: client, logger: client.logger}, nil
}

// Push applies the per-instance policy of plan §7.3:
//
//   - online (1)   → upsert register, enabled per Enabled
//   - unhealthy (2)→ upsert register, enabled=false (the consul "old
//     instance, mark not-ready" semantics — Status 2 is pushed, not deleted)
//   - offline (3)  → deregister, DELETE by composite id
//   - unknown (0)  → skipped defensively (upstream filters already reject it)
//
// Instances are pushed sequentially.
func (s *Sink) Push(triggerTime int64, instances []*instance.Instance) error {
	var firstErr error
	for _, ins := range instances {
		if ins == nil {
			continue
		}
		if err := s.pushOne(ins); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PushAll upserts every pushed instance and then prunes: for every
// (serviceName, clusterName) pair present in the pushed set it lists
// Nacos's current instances of that service and DELETEs every remote
// instance of that cluster not in the pushed set. The prune is scoped to
// clusters present in the data, so a k8s full push never touches
// ecs-cluster instances — per-provider ownership is preserved end-to-end
// (plan §7.4).
func (s *Sink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	if err := s.Push(triggerTime, instances); err != nil {
		return err
	}
	return s.prune(instances)
}

// prune removes the remote instances of the pushed (service, cluster) pairs
// that the pushed set no longer contains. Offline (Status 3) pushed
// instances are treated as absent, so their remote counterparts are pruned
// even if the per-instance deregister failed to run.
func (s *Sink) prune(instances []*instance.Instance) error {
	type clusterKey struct {
		service string
		cluster string
	}

	desired := map[clusterKey]map[string]bool{}
	for _, ins := range instances {
		if ins == nil {
			continue
		}
		key := clusterKey{service: ins.AppCode, cluster: clusterOf(ins)}
		if desired[key] == nil {
			desired[key] = map[string]bool{}
		}
		if ins.Status != instance.InstanceStatusOffline {
			desired[key][compositeID(ins)] = true
		}
	}

	var firstErr error
	for key, wanted := range desired {
		hosts, err := s.client.ListInstances(key.service)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("nacos: prune list %s/%s: %w", key.service, key.cluster, err)
			}
			continue
		}
		for _, host := range hosts {
			if host.ClusterName != key.cluster {
				continue // another provider's cluster: never pruned here
			}
			if wanted[host.InstanceID] {
				continue
			}
			if err := s.deregister(key.service, host.IP, host.Port, key.cluster); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// GetAll reconstructs domain instances from Nacos: list all services of the
// group (paginated), list each service's instances, keep the ones whose
// clusterName matches the requested provider, and rebuild the instance from
// the composite fields plus the metadata map. Status filtering uses the
// status metadata with an enabled→1/else→2 fallback (plan §7.3).
//
// Fidelity limits, documented as acceptable: ports beyond the first, labels
// and images are not recoverable; v1 comparisons never run against the
// Nacos view (plan §3.3).
func (s *Sink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	services, err := s.client.ListServices(100)
	if err != nil {
		return nil, err
	}
	instances := []*instance.Instance{}
	for _, service := range services {
		hosts, err := s.client.ListInstances(service)
		if err != nil {
			return nil, err
		}
		for _, host := range hosts {
			if provider != "" && host.ClusterName != provider {
				continue
			}
			ins := reconstruct(service, host)
			if !statusAllowed(statuses, ins.Status) {
				continue
			}
			instances = append(instances, ins)
		}
	}
	return &instance.InstanceList{Instance: instances}, nil
}

// Close releases the sink's resources. The HTTP client is stateless, so
// this is a no-op kept for the construction/cleanup symmetry of the server
// wiring (plan §6.5); it is safe to call repeatedly.
func (s *Sink) Close() error {
	return nil
}

// pushOne applies the per-instance policy for one instance.
func (s *Sink) pushOne(ins *instance.Instance) error {
	switch ins.Status {
	case instance.InstanceStatusOffline:
		return s.deregister(ins.AppCode, ins.Ip, firstPort(ins), clusterOf(ins))
	case instance.InstanceStatusOnline, instance.InstanceStatusUnhealthy:
		return s.register(ins)
	default:
		// Status 0 (unknown): upstream filters reject it (rules.go:99-101);
		// skip defensively rather than assert unknown state remotely.
		s.logger.Warnf("nacos: skipping instance %s with unknown status %d", ins.InstanceId, ins.Status)
		return nil
	}
}

// register upserts one instance: the enabled flag follows the status
// policy — Enabled for online, forced false for unhealthy.
func (s *Sink) register(ins *instance.Instance) error {
	enabled := ins.Enabled
	if ins.Status == instance.InstanceStatusUnhealthy {
		enabled = false
	}
	err := s.client.RegisterInstance(InstanceParams{
		ServiceName: ins.AppCode,
		IP:          ins.Ip,
		Port:        firstPort(ins),
		ClusterName: clusterOf(ins),
		Enabled:     enabled,
		Ephemeral:   false, // persistent: the sink owns the lifecycle (§7.1)
		Metadata:    metadataOf(ins),
	})
	if err != nil {
		return fmt.Errorf("nacos: register %s: %w", ins.InstanceId, err)
	}
	s.logger.Infof("nacos: registered instance %s as %s", ins.InstanceId, compositeID(ins))
	return nil
}

// deregister deletes one instance by its composite id parameters — the
// same derivation the register used, so the ids round-trip.
func (s *Sink) deregister(service, ip string, port int, cluster string) error {
	err := s.client.DeregisterInstance(InstanceParams{
		ServiceName: service,
		IP:          ip,
		Port:        port,
		ClusterName: cluster,
		Ephemeral:   false,
	})
	if err != nil {
		return fmt.Errorf("nacos: deregister %s#%d/%s: %w", ip, port, cluster, err)
	}
	s.logger.Infof("nacos: deregistered instance %s#%d/%s", ip, port, cluster)
	return nil
}

// clusterOf maps the provider to the Nacos clusterName: the collision
// policy of plan §7.3 — the same app-code from k8s and ecs coexists in one
// service under distinct clusters, yielding distinct composite ids.
func clusterOf(ins *instance.Instance) string {
	if ins.Provider != "" {
		return ins.Provider
	}
	return DefaultCluster
}

// firstPort derives the wire port: Ports[0].Port, else 0 — the same rule
// on register and deregister so the composite id round-trips (plan §7.3).
func firstPort(ins *instance.Instance) int {
	if len(ins.Ports) > 0 && ins.Ports[0] != nil {
		return int(ins.Ports[0].Port)
	}
	return 0
}

// compositeID derives the Nacos composite instance id of a domain instance
// (ip#port#cluster#group@@service), the identity the prune compares remote
// hosts against.
func compositeID(ins *instance.Instance) string {
	return fmt.Sprintf("%s#%d#%s#%s@@%s", ins.Ip, firstPort(ins), clusterOf(ins), DefaultGroup, ins.AppCode)
}

// metadataOf renders the metadata map carried for the GetAll round-trip
// (plan §7.3): identity and every compared field, as strings.
func metadataOf(ins *instance.Instance) map[string]string {
	metadata := map[string]string{
		"instanceId": ins.InstanceId,
		"envType":    ins.EnvType,
		"envGroup":   ins.EnvGroup,
		"reversion":  strconv.FormatInt(ins.Reversion, 10),
		"status":     strconv.FormatInt(int64(ins.Status), 10),
		"state":      ins.State,
		"idc":        ins.Idc,
		"cpu":        strconv.FormatFloat(float64(ins.Cpu), 'f', -1, 32),
		"version":    ins.Version,
	}
	return metadata
}

// reconstruct rebuilds a domain instance from one remote host. The status
// comes from the metadata with the enabled→online/else→unhealthy fallback
// (plan §7.3), so unmetadataed state still classifies.
func reconstruct(service string, host Host) *instance.Instance {
	cluster := host.ClusterName
	status := parseStatus(host.Metadata["status"], host.Enabled)
	ins := &instance.Instance{
		InstanceId: host.Metadata["instanceId"],
		AppCode:    service,
		Ip:         host.IP,
		Ports:      []*instance.PortInfo{{Port: int32(host.Port)}},
		Provider:   cluster,
		Cluster:    cluster,
		Enabled:    host.Enabled,
		EnvType:    host.Metadata["envType"],
		EnvGroup:   host.Metadata["envGroup"],
		State:      host.Metadata["state"],
		Idc:        host.Metadata["idc"],
		Version:    host.Metadata["version"],
		Reversion:  parseInt64(host.Metadata["reversion"]),
		Status:     status,
	}
	if cpu, err := strconv.ParseFloat(host.Metadata["cpu"], 32); err == nil {
		ins.Cpu = float32(cpu)
	}
	return ins
}

// parseStatus resolves the instance status: the metadata status when
// present, else enabled→online (1) / disabled→unhealthy (2).
func parseStatus(raw string, enabled bool) int32 {
	if raw != "" {
		if status, err := strconv.ParseInt(raw, 10, 32); err == nil {
			return int32(status)
		}
	}
	if enabled {
		return instance.InstanceStatusOnline
	}
	return instance.InstanceStatusUnhealthy
}

// statusAllowed reports whether status is selected: an empty status list
// selects everything (the caller's "" statuses convention).
func statusAllowed(statuses []int32, status int32) bool {
	if len(statuses) == 0 {
		return true
	}
	for _, wanted := range statuses {
		if wanted == status {
			return true
		}
	}
	return false
}

// parseInt64 parses a metadata integer field, defaulting to zero.
func parseInt64(raw string) int64 {
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}
