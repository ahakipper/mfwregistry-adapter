package instance

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// canonicalPayloadVersion identifies the complete Spotter instance payload
// stored in a Nacos metadata value. The payload is deliberately versioned so
// future fields can be added without silently treating an older writer as a
// complete equality proof.
const canonicalPayloadVersion = 1

// canonicalPayload is the round-trippable domain projection used by sinks and
// reconciliation. It contains every Instance property; Nacos-owned health is
// not an Instance property and therefore is intentionally absent.
type canonicalPayload struct {
	SchemaVersion int               `json:"schemaVersion"`
	SourceKey     string            `json:"sourceKey"`
	SourceCluster string            `json:"sourceCluster"`
	InstanceID    string            `json:"instanceId"`
	Level         string            `json:"level"`
	Ports         []*PortInfo       `json:"ports"`
	IP            string            `json:"ip"`
	EnvCode       string            `json:"envCode"`
	EnvType       string            `json:"envType"`
	EnvGroup      string            `json:"envGroup"`
	Cluster       string            `json:"cluster"`
	Version       string            `json:"version"`
	Enabled       bool              `json:"enabled"`
	State         string            `json:"state"`
	HealthState   string            `json:"healthState"`
	AppCode       string            `json:"appCode"`
	Provider      string            `json:"provider"`
	Label         map[string]string `json:"label"`
	Hostname      string            `json:"hostname"`
	CPU           float32           `json:"cpu"`
	Memory        int32             `json:"memory"`
	Disk          int32             `json:"disk"`
	OS            string            `json:"os"`
	Image         map[string]string `json:"image"`
	IDc           string            `json:"idc"`
	Reversion     int64             `json:"reversion"`
	Status        int32             `json:"status"`
}

// CanonicalPayload serializes every domain Instance property into a stable
// JSON value suitable for a string-valued discovery metadata map. Go's JSON
// encoder sorts map keys, and nil maps/slices are normalized to empty values,
// so equal domain instances produce byte-identical payloads.
func CanonicalPayload(ins *Instance) string {
	if ins == nil {
		return ""
	}
	payload := canonicalPayload{
		SchemaVersion: canonicalPayloadVersion,
		SourceKey:     ins.SourceKey, SourceCluster: ins.SourceCluster,
		InstanceID: ins.InstanceId, Level: ins.Level,
		Ports: copyPorts(ins.Ports), IP: ins.Ip, EnvCode: ins.EnvCode,
		EnvType: ins.EnvType, EnvGroup: ins.EnvGroup, Cluster: ins.Cluster,
		Version: ins.Version, Enabled: ins.Enabled, State: ins.State,
		HealthState: ins.HealthState, AppCode: ins.AppCode, Provider: ins.Provider,
		Label: copyMap(ins.Label), Hostname: ins.Hostname, CPU: ins.Cpu,
		Memory: ins.Memory, Disk: ins.Disk, OS: ins.Os, Image: copyMap(ins.Image),
		IDc: ins.Idc, Reversion: ins.Reversion, Status: ins.Status,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		// canonicalPayload contains only JSON-safe scalar/string/map/slice
		// fields, so this is unreachable unless the type changes incorrectly.
		panic(fmt.Sprintf("instance: marshal canonical payload: %v", err))
	}
	return string(data)
}

// DecodeCanonicalPayload reconstructs an Instance from the complete payload
// written by CanonicalPayload. Unknown or unsupported payload versions are
// rejected so callers can fall back to the legacy metadata path and trigger a
// safe re-publication instead of claiming equality with an incomplete schema.
func DecodeCanonicalPayload(raw string) (*Instance, error) {
	if raw == "" {
		return nil, fmt.Errorf("instance: canonical payload is empty")
	}
	var payload canonicalPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("instance: decode canonical payload: %w", err)
	}
	if payload.SchemaVersion != canonicalPayloadVersion {
		return nil, fmt.Errorf("instance: unsupported canonical payload version %d", payload.SchemaVersion)
	}
	return &Instance{
		SourceKey: payload.SourceKey, SourceCluster: payload.SourceCluster,
		InstanceId: payload.InstanceID, Level: payload.Level,
		Ports: copyPorts(payload.Ports), Ip: payload.IP, EnvCode: payload.EnvCode,
		EnvType: payload.EnvType, EnvGroup: payload.EnvGroup, Cluster: payload.Cluster,
		Version: payload.Version, Enabled: payload.Enabled, State: payload.State,
		HealthState: payload.HealthState, AppCode: payload.AppCode, Provider: payload.Provider,
		Label: copyMap(payload.Label), Hostname: payload.Hostname, Cpu: payload.CPU,
		Memory: payload.Memory, Disk: payload.Disk, Os: payload.OS,
		Image: copyMap(payload.Image), Idc: payload.IDc, Reversion: payload.Reversion,
		Status: payload.Status,
	}, nil
}

// CompressedCanonicalPayload encodes the complete payload in a compact
// URL-safe base64 form. Nacos limits the serialized metadata parameter to
// 1024 bytes; compression keeps a normal K8s instance (including labels,
// ports, image and resource fields) below that limit while retaining every
// property for equality and reconstruction.
func CompressedCanonicalPayload(ins *Instance) string {
	raw := CanonicalPayload(ins)
	if raw == "" {
		return ""
	}
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write([]byte(raw)); err != nil {
		panic(fmt.Sprintf("instance: compress canonical payload: %v", err))
	}
	if err := writer.Close(); err != nil {
		panic(fmt.Sprintf("instance: close compressed canonical payload: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(compressed.Bytes())
}

// DecodeCompressedCanonicalPayload reverses CompressedCanonicalPayload.
func DecodeCompressedCanonicalPayload(raw string) (*Instance, error) {
	if raw == "" {
		return nil, fmt.Errorf("instance: compressed canonical payload is empty")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("instance: decode compressed payload: %w", err)
	}
	reader, err := zlib.NewReader(bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("instance: open compressed payload: %w", err)
	}
	decompressed := new(bytes.Buffer)
	if _, err := decompressed.ReadFrom(reader); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("instance: decompress canonical payload: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("instance: close compressed payload: %w", err)
	}
	return DecodeCanonicalPayload(decompressed.String())
}

func copyMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func copyPorts(input []*PortInfo) []*PortInfo {
	output := make([]*PortInfo, 0, len(input))
	for _, port := range input {
		if port == nil {
			output = append(output, nil)
			continue
		}
		copy := *port
		output = append(output, &copy)
	}
	return output
}
