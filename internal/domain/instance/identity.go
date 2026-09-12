package instance

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
