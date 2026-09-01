package instance

import (
	"reflect"
	"testing"
)

func TestDiffNewerReversion(t *testing.T) {
	tests := []struct {
		name string
		old  *Instance
		new  *Instance
		want bool
	}{
		{
			name: "both offline ignores newer revision",
			old:  &Instance{Status: InstanceStatusOffline, Reversion: 1},
			new:  &Instance{Status: InstanceStatusOffline, Reversion: 2},
		},
		{
			name: "newer revision differs",
			old:  &Instance{Status: InstanceStatusOnline, Reversion: 1},
			new:  &Instance{Status: InstanceStatusOnline, Reversion: 2},
			want: true,
		},
		{
			name: "selected fields differ at an older revision",
			old:  &Instance{Reversion: 2, EnvType: "test"},
			new:  &Instance{Reversion: 1, EnvType: "prod"},
			want: true,
		},
		{
			name: "cpu and memory are ignored",
			old:  &Instance{Reversion: 2, Cpu: 1, Memory: 128},
			new:  &Instance{Reversion: 2, Cpu: 2, Memory: 256},
		},
		{
			name: "same instance has no difference",
			old:  &Instance{InstanceId: "one", Reversion: 2, EnvType: "test", State: InstanceStateRunning, Status: InstanceStatusOnline, EnvGroup: "a", Ip: "127.0.0.1"},
			new:  &Instance{InstanceId: "one", Reversion: 2, EnvType: "test", State: InstanceStateRunning, Status: InstanceStatusOnline, EnvGroup: "a", Ip: "127.0.0.1"},
		},
		{
			name: "nil input is not a difference",
			old:  nil,
			new:  &Instance{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DiffNewerReversion(tt.old, tt.new); got != tt.want {
				t.Fatalf("DiffNewerReversion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffEqualReversion(t *testing.T) {
	base := Instance{
		InstanceId: "one",
		Reversion:  3,
		EnvType:    "test",
		EnvGroup:   "blue",
		Status:     InstanceStatusOnline,
		State:      InstanceStateRunning,
		Ip:         "127.0.0.1",
		Idc:        "idc-a",
		Cluster:    "cluster-a",
		Enabled:    true,
		AppCode:    "app",
		Cpu:        1,
		Memory:     128,
	}

	tests := []struct {
		name   string
		mutate func(*Instance)
		want   bool
	}{
		{name: "identical instances", want: false},
		{name: "different revision is ignored by equal revision policy", mutate: func(ins *Instance) { ins.Reversion++ }, want: false},
		{name: "environment type differs", mutate: func(ins *Instance) { ins.EnvType = "prod" }, want: true},
		{name: "environment group differs", mutate: func(ins *Instance) { ins.EnvGroup = "green" }, want: true},
		{name: "status differs", mutate: func(ins *Instance) { ins.Status = InstanceStatusUnhealthy }, want: true},
		{name: "state differs", mutate: func(ins *Instance) { ins.State = InstanceStateProbing }, want: true},
		{name: "ip differs", mutate: func(ins *Instance) { ins.Ip = "127.0.0.2" }, want: true},
		{name: "idc differs", mutate: func(ins *Instance) { ins.Idc = "idc-b" }, want: true},
		{name: "cluster differs", mutate: func(ins *Instance) { ins.Cluster = "cluster-b" }, want: true},
		{name: "enabled differs", mutate: func(ins *Instance) { ins.Enabled = false }, want: true},
		{name: "app code differs", mutate: func(ins *Instance) { ins.AppCode = "other" }, want: true},
		{name: "cpu differs", mutate: func(ins *Instance) { ins.Cpu = 2 }, want: true},
		{name: "memory is ignored", mutate: func(ins *Instance) { ins.Memory = 256 }, want: false},
		{name: "other fields are ignored", mutate: func(ins *Instance) { ins.Version = "new"; ins.Hostname = "host" }, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := base
			newInstance := base
			if tt.mutate != nil {
				tt.mutate(&newInstance)
			}
			if got := DiffEqualReversion(&old, &newInstance); got != tt.want {
				t.Fatalf("DiffEqualReversion() = %v, want %v", got, tt.want)
			}
		})
	}

	if DiffEqualReversion(nil, &base) {
		t.Fatal("DiffEqualReversion(nil, instance) = true, want false")
	}
	if DiffEqualReversion(&base, nil) {
		t.Fatal("DiffEqualReversion(instance, nil) = true, want false")
	}
}

func TestCompareThreeWay(t *testing.T) {
	providerOnly := &Instance{InstanceId: "provider-only"}
	remoteOnly := &Instance{InstanceId: "remote-only"}
	providerSecond := &Instance{InstanceId: "provider-second"}
	remoteSecond := &Instance{InstanceId: "remote-second"}
	newerChanged := &Instance{InstanceId: "newer-changed", Reversion: 2}
	equalChanged := &Instance{InstanceId: "equal-changed", Reversion: 4, Cpu: 2}

	tests := []struct {
		name         string
		policy       DiffPolicy
		provider     []*Instance
		remote       []*Instance
		wantChanged  []*Instance
		wantProvider []*Instance
		wantRemote   []*Instance
	}{
		{
			name:         "newer revision policy uses only newer policy",
			policy:       DiffNewerReversion,
			provider:     []*Instance{newerChanged, providerOnly, equalChanged, providerSecond},
			remote:       []*Instance{{InstanceId: "newer-changed", Reversion: 1}, remoteOnly, {InstanceId: "equal-changed", Reversion: 4, Cpu: 1}, remoteSecond},
			wantChanged:  []*Instance{newerChanged},
			wantProvider: []*Instance{providerOnly, providerSecond},
			wantRemote:   []*Instance{remoteOnly, remoteSecond},
		},
		{
			name:         "equal revision policy uses only equal policy",
			policy:       DiffEqualReversion,
			provider:     []*Instance{newerChanged, providerOnly, equalChanged, providerSecond},
			remote:       []*Instance{{InstanceId: "newer-changed", Reversion: 1}, remoteOnly, {InstanceId: "equal-changed", Reversion: 4, Cpu: 1}, remoteSecond},
			wantChanged:  []*Instance{equalChanged},
			wantProvider: []*Instance{providerOnly, providerSecond},
			wantRemote:   []*Instance{remoteOnly, remoteSecond},
		},
		{
			name:         "nil policy treats matching IDs as unchanged",
			provider:     []*Instance{newerChanged, providerOnly, providerSecond},
			remote:       []*Instance{{InstanceId: "newer-changed", Reversion: 1}, remoteOnly, remoteSecond},
			wantProvider: []*Instance{providerOnly, providerSecond},
			wantRemote:   []*Instance{remoteOnly, remoteSecond},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProviderOnly, gotRemoteOnly, gotChanged := CompareThreeWay(tt.provider, tt.remote, tt.policy)
			if !reflect.DeepEqual(gotProviderOnly, tt.wantProvider) {
				t.Fatalf("provider-only = %#v, want provider input order %#v", gotProviderOnly, tt.wantProvider)
			}
			if !reflect.DeepEqual(gotRemoteOnly, tt.wantRemote) {
				t.Fatalf("remote-only = %#v, want remote input order %#v", gotRemoteOnly, tt.wantRemote)
			}
			if !reflect.DeepEqual(gotChanged, tt.wantChanged) {
				t.Fatalf("changed = %#v, want provider input order %#v", gotChanged, tt.wantChanged)
			}
		})
	}
}

func TestListToMap(t *testing.T) {
	first := &Instance{InstanceId: "one", Version: "first"}
	last := &Instance{InstanceId: "one", Version: "last"}
	second := &Instance{InstanceId: "two"}

	got := ListToMap([]*Instance{first, nil, last, second})
	if len(got) != 2 {
		t.Fatalf("len(ListToMap()) = %d, want 2", len(got))
	}
	if got["one"] != last || got["two"] != second {
		t.Fatalf("ListToMap() = %#v, want last duplicate and all non-nil instances", got)
	}
}

func TestInitInstanceFilters(t *testing.T) {
	filters := InitInstanceFilters()
	if len(filters) != 1 {
		t.Fatalf("len(InitInstanceFilters()) = %d, want 1", len(filters))
	}

	tests := []struct {
		name string
		ins  *Instance
		want string
	}{
		{name: "valid", ins: &Instance{AppCode: "app", EnvType: "test", Status: InstanceStatusOnline, Ip: "127.0.0.1", Reversion: 1}},
		{name: "nil instance", want: "nil resource instance"},
		{name: "empty app code", ins: &Instance{EnvType: "test", Reversion: 1}, want: "instance has nil appcode"},
		{name: "empty environment type", ins: &Instance{AppCode: "app", Reversion: 1}, want: "instance has nil env type"},
		{name: "online without ip", ins: &Instance{AppCode: "app", EnvType: "test", Status: InstanceStatusOnline, Reversion: 1}, want: "instance has nil ip when it on online status"},
		{name: "pending", ins: &Instance{AppCode: "app", EnvType: "test", State: InstanceStatePending, Reversion: 1}, want: "instance has nil ip when it on heal check status and pending state"},
		{name: "zero revision", ins: &Instance{AppCode: "app", EnvType: "test"}, want: "instance has nil reversion"},
		{name: "unknown status", ins: &Instance{AppCode: "app", EnvType: "test", Reversion: 1, Status: InstanceStatusUnknown}, want: "instance has status unknown value: 0, may be the format process need to be performed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := filters[0](tt.ins)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("filter() error = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tt.want {
				t.Fatalf("filter() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestComposeEnvCode(t *testing.T) {
	tests := []struct {
		name     string
		envType  string
		envGroup string
		want     string
	}{
		{name: "type and group", envType: "test", envGroup: "blue", want: "test#blue"},
		{name: "empty group is retained", envType: "prod", want: "prod#"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComposeEnvCode(tt.envType, tt.envGroup); got != tt.want {
				t.Fatalf("ComposeEnvCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestObservationRules(t *testing.T) {
	tests := []struct {
		name   string
		obs    PodObservation
		state  string
		status int32
	}{
		{name: "deletion event only changes status", obs: PodObservation{Phase: "Running", Event: "delete", ContainerReady: true, ContainerRunning: true}, state: InstanceStateRunning, status: InstanceStatusOffline},
		{name: "deleting pod", obs: PodObservation{Phase: "Running", Deleting: true}, state: InstanceStateTerminated, status: InstanceStatusOffline},
		{name: "pending pod", obs: PodObservation{Phase: "Pending"}, state: InstanceStatePending, status: InstanceStatusUnhealthy},
		{name: "unknown pod", obs: PodObservation{Phase: "Unknown"}, state: InstanceStateUnknown, status: InstanceStatusOffline},
		{name: "evicted pod", obs: PodObservation{Phase: "Failed", PodReason: "Evicted"}, state: InstanceStateEvicted, status: InstanceStatusUnhealthy},
		{name: "failure ignores labels and event reasons", obs: PodObservation{Phase: "Failed", Labels: map[string]string{"reason": "Evicted"}, Event: "Evicted"}, state: InstanceStateFailed, status: InstanceStatusOffline},
		{name: "failed pod", obs: PodObservation{Phase: "Failed"}, state: InstanceStateFailed, status: InstanceStatusOffline},
		{name: "succeeded pod", obs: PodObservation{Phase: "Succeeded"}, state: InstanceStateTerminated, status: InstanceStatusOffline},
		{name: "ready running pod", obs: PodObservation{Phase: "Running", ContainerReady: true, ContainerRunning: true}, state: InstanceStateRunning, status: InstanceStatusOnline},
		{name: "ready state ignores container running", obs: PodObservation{Phase: "Running", ContainerReady: true}, state: InstanceStateRunning, status: InstanceStatusUnhealthy},
		{name: "not ready running pod", obs: PodObservation{Phase: "Running", ContainerRunning: true}, state: InstanceStateProbing, status: InstanceStatusUnhealthy},
		{name: "crash loop", obs: PodObservation{Phase: "Running", WaitingReason: "CrashLoopBackOff"}, state: InstanceStateCrash, status: InstanceStatusUnhealthy},
		{name: "crash loop after error", obs: PodObservation{Phase: "Running", WaitingReason: "CrashLoopBackOff", LastTerminationReason: "Error"}, state: InstanceStateError, status: InstanceStatusUnhealthy},
		{name: "running failure ignores labels and event reasons", obs: PodObservation{Phase: "Running", Event: "CrashLoopBackOff", Labels: map[string]string{"termination-reason": "Error"}}, state: InstanceStateProbing, status: InstanceStatusUnhealthy},
		{name: "ready state wins over waiting reasons", obs: PodObservation{Phase: "Running", ContainerReady: true, WaitingReason: "CrashLoopBackOff", LastTerminationReason: "Error"}, state: InstanceStateRunning, status: InstanceStatusUnhealthy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StateOf(tt.obs); got != tt.state {
				t.Errorf("StateOf() = %q, want %q", got, tt.state)
			}
			if got := StatusOf(tt.obs); got != tt.status {
				t.Errorf("StatusOf() = %d, want %d", got, tt.status)
			}
		})
	}
}

func TestEnvTypeOf(t *testing.T) {
	tests := []struct {
		name       string
		obs        PodObservation
		discovered string
		want       string
	}{
		{name: "lowercases discovered value", discovered: "TeSt", want: EnvTest},
		{name: "env type label overrides discovered", obs: PodObservation{Labels: map[string]string{"env-type": "STAGING"}}, discovered: "dev", want: EnvStaging},
		{name: "cluster type label fills empty discovered", obs: PodObservation{Labels: map[string]string{"K8S_CLUSTER_TYPE": "TEST"}}, want: EnvTest},
		{name: "env type label does not fill empty discovered", obs: PodObservation{Labels: map[string]string{"env-type": "STAGING"}}, want: ""},
		{name: "cluster type label wins when discovered is empty", obs: PodObservation{Labels: map[string]string{"env-type": "STAGING", "K8S_CLUSTER_TYPE": "TEST"}}, want: EnvTest},
		{name: "cluster type environment is ignored", obs: PodObservation{Env: map[string]string{"K8S_CLUSTER_TYPE": "DEV"}}, want: ""},
		{name: "cluster type label maps online to product", obs: PodObservation{Labels: map[string]string{"K8S_CLUSTER_TYPE": "ONLINE"}}, want: EnvProduct},
		{name: "online maps to product", discovered: "ONLINE", want: EnvProduct},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EnvTypeOf(tt.obs, tt.discovered); got != tt.want {
				t.Fatalf("EnvTypeOf() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestModelGetters(t *testing.T) {
	var response *CommonResponse
	if response.GetCode() != 0 || response.GetMsg() != "" {
		t.Fatal("nil CommonResponse getters are not zero-valued")
	}
	response = &CommonResponse{Code: 7, Msg: "ok"}
	if response.GetCode() != 7 || response.GetMsg() != "ok" {
		t.Fatal("CommonResponse getters did not return fields")
	}

	items := []*Instance{{InstanceId: "one"}}
	list := &InstanceList{Instance: items}
	if !reflect.DeepEqual(list.GetInstance(), items) {
		t.Fatalf("InstanceList.GetInstance() = %#v, want %#v", list.GetInstance(), items)
	}
	var nilList *InstanceList
	if nilList.GetInstance() != nil {
		t.Fatal("nil InstanceList.GetInstance() is not nil")
	}
}
