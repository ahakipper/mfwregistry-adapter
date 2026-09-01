package instance

// Instance is the provider-independent instance model.
type Instance struct {
	InstanceId  string
	Level       string
	Ports       []*PortInfo
	Ip          string
	EnvCode     string
	EnvType     string
	EnvGroup    string
	Cluster     string
	Version     string
	Enabled     bool
	State       string
	HealthState string
	AppCode     string
	Provider    string
	Label       map[string]string
	Hostname    string
	Cpu         float32
	Memory      int32
	Disk        int32
	Os          string
	Image       map[string]string
	Idc         string
	Reversion   int64
	Status      int32
}

// PortInfo describes a network port exposed by an instance.
type PortInfo struct {
	Name        string
	Protocol    string
	Port        int32
	ServicePort int32
}

// InstanceList contains instances returned by a remote source.
type InstanceList struct {
	Instance []*Instance
}

// GetInstance returns the contained instances, including for a nil list.
func (l *InstanceList) GetInstance() []*Instance {
	if l == nil {
		return nil
	}
	return l.Instance
}

// CommonResponse contains the result of a remote operation.
type CommonResponse struct {
	Code int32
	Msg  string
}

// GetCode returns the response code, or zero for a nil response.
func (r *CommonResponse) GetCode() int32 {
	if r == nil {
		return 0
	}
	return r.Code
}

// GetMsg returns the response message, or an empty string for a nil response.
func (r *CommonResponse) GetMsg() string {
	if r == nil {
		return ""
	}
	return r.Msg
}

// SynInstancesRequest contains instances for an incremental synchronization.
type SynInstancesRequest struct {
	Instance []*Instance
}

// SynAllInstancesRequest contains instances for a full synchronization.
type SynAllInstancesRequest struct {
	Instance []*Instance
}

// GetAllInstancesRequest selects instances by status and provider.
type GetAllInstancesRequest struct {
	Status   int32
	Provider string
}

// RuntimeConfig contains runtime resource and environment settings.
type RuntimeConfig struct {
	Cpu          float32           `json:"cpu"`
	Memory       int32             `json:"memory"`
	Image        map[string]string `json:"image"`
	Environments map[string]string `json:"environments"`
}
