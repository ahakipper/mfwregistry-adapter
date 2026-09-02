package config

import (
	"fmt"
	"reflect"
	"testing"
)

// boolPtr is a helper for building tri-state LeaderElection flag values.
func boolPtr(v bool) *bool { return &v }

func TestPresetFor(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want Endpoints
	}{
		{
			name: "test environment",
			env:  "test",
			want: Endpoints{
				EtcdEndpoints: []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"},
				CertFile:      "./config/certs/etcdtest/etcd.pem",
				KeyFile:       "./config/certs/etcdtest/etcd-key.pem",
				CAFile:        "./config/certs/etcdtest/ca.pem",
				KubeConfigPath: []string{
					"./config/kubeconfigs/k8s-sailor",
				},
				ConsulAddress:   []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"},
				Providers:       []string{},
				LockCampaignKey: "/paas/spotter-test",
			},
		},
		{
			name: "dev environment",
			env:  "dev",
			want: Endpoints{
				EtcdEndpoints: []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"},
				CertFile:      "./config/certs/etcdtest/etcd.pem",
				KeyFile:       "./config/certs/etcdtest/etcd-key.pem",
				CAFile:        "./config/certs/etcdtest/ca.pem",
				KubeConfigPath: []string{
					"./config/kubeconfigs/k8s-sailor",
					"./config/kubeconfigs/k8s-vipper",
				},
				ConsulAddress:   []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"},
				Providers:       []string{},
				LockCampaignKey: "/paas/spotter",
			},
		},
		{
			name: "product environment",
			env:  "product",
			want: Endpoints{
				EtcdEndpoints: []string{"192.168.11.100:2479", "192.168.11.101:2479", "192.168.11.102:2479"},
				CertFile:      "./config/certs/etcdprod/etcd.pem",
				KeyFile:       "./config/certs/etcdprod/etcd-key.pem",
				CAFile:        "./config/certs/etcdprod/ca.pem",
				KubeConfigPath: []string{
					"./config/kubeconfigs/k8s-eel",
					"./config/kubeconfigs/k8s-otter",
					"./config/kubeconfigs/k8s-slug",
					"./config/kubeconfigs/k8s-bernuda",
				},
				ConsulAddress:   []string{"10.132.2.40:8520", "10.132.2.42:8520", "10.132.2.43:8520"},
				Providers:       []string{},
				LockCampaignKey: "/paas/spotter",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := PresetFor(tt.env)
			if err != nil {
				t.Fatalf("PresetFor(%q) returned error: %v", tt.env, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PresetFor(%q) =\n%+v\nwant\n%+v", tt.env, got, tt.want)
			}
		})
	}
}

func TestPresetForCaseInsensitive(t *testing.T) {
	tests := []struct {
		env  string
		want Endpoints
	}{
		{env: "TEST", want: testPreset()},
		{env: "Test", want: testPreset()},
		{env: "DEV", want: devPreset()},
		{env: "Dev", want: devPreset()},
		{env: "PRODUCT", want: prodPreset()},
		{env: "Product", want: prodPreset()},
		{env: "tEsT", want: testPreset()},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.env, func(t *testing.T) {
			got, err := PresetFor(tt.env)
			if err != nil {
				t.Fatalf("PresetFor(%q) returned error: %v", tt.env, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PresetFor(%q) = %+v, want %+v", tt.env, got, tt.want)
			}
		})
	}
}

func TestPresetForInvalidEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{name: "empty env", env: ""},
		{name: "prod abbreviation is not accepted", env: "prod"},
		{name: "unknown env", env: "staging"},
		{name: "production is not accepted", env: "production"},
		{name: "surrounding spaces are not trimmed", env: "test "},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := PresetFor(tt.env)
			if err == nil {
				t.Fatalf("PresetFor(%q) succeeded, want error", tt.env)
			}
			if want := fmt.Sprintf("unsupported environment: %q", tt.env); err.Error() != want {
				t.Errorf("PresetFor(%q) error = %q, want %q", tt.env, err.Error(), want)
			}
			if !reflect.DeepEqual(got, Endpoints{}) {
				t.Errorf("PresetFor(%q) = %+v, want zero Endpoints", tt.env, got)
			}
		})
	}
}

func TestLoadDefaults(t *testing.T) {
	wantEndpoints, err := PresetFor("test")
	if err != nil {
		t.Fatalf("PresetFor(\"test\") failed: %v", err)
	}

	// Only providers is set; every other flag keeps its zero value so the
	// defaults must be applied.
	flags := Flags{Providers: []string{"k8s"}}

	got, err := Load("test", flags)
	if err != nil {
		t.Fatalf("Load(\"test\", flags) returned error: %v", err)
	}

	want := Config{
		Endpoints:            wantEndpoints,
		Env:                  "test",
		LogFilePath:          "./logfiles/",
		LogSize:              100,
		LogLevel:             -1,
		LogBackups:           10,
		LogAge:               7,
		LogEncoding:          "json",
		LogToStd:             true,
		PushAllInterval:      21600,
		GrpcAddr:             "172.16.130.71:50051",
		DisablePushWorker:    false,
		Providers:            []string{"k8s"},
		PushAppCodes:         []string{},
		EnableLeaderElection: true,
		MetricsAddr:          ":8090",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load(\"test\", flags) =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadFlagValues(t *testing.T) {
	wantEndpoints, err := PresetFor("product")
	if err != nil {
		t.Fatalf("PresetFor(\"product\") failed: %v", err)
	}

	flags := Flags{
		LogFilePath:       "/var/log/spotter",
		LogSize:           5,
		LogLevel:          1,
		LogBackups:        3,
		LogAge:            14,
		LogEncoding:       "log",
		LogToStd:          true,
		PushAllInterval:   60,
		GrpcAddr:          "127.0.0.1:50052",
		DisablePushWorker: true,
		Providers:         []string{"k8s", "consul"},
		PushAppCodes:      []string{"app-code-1", "app-code-2"},
		LeaderElection:    boolPtr(false),
		MetricsAddr:       ":9091",
	}

	got, err := Load("product", flags)
	if err != nil {
		t.Fatalf("Load(\"product\", flags) returned error: %v", err)
	}

	want := Config{
		Endpoints:            wantEndpoints,
		Env:                  "product",
		LogFilePath:          "/var/log/spotter",
		LogSize:              5,
		LogLevel:             1,
		LogBackups:           3,
		LogAge:               14,
		LogEncoding:          "log",
		LogToStd:             true,
		PushAllInterval:      60,
		GrpcAddr:             "127.0.0.1:50052",
		DisablePushWorker:    true,
		Providers:            []string{"k8s", "consul"},
		PushAppCodes:         []string{"app-code-1", "app-code-2"},
		EnableLeaderElection: false,
		MetricsAddr:          ":9091",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load(\"product\", flags) =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadProviderFlagOverridesPreset(t *testing.T) {
	// Every built-in preset ships an empty provider list, so the flag list
	// is the effective source; it must be used as given, after trimming.
	tests := []struct {
		name     string
		provider []string
		want     []string
	}{
		{
			name:     "flag list wins over empty preset",
			provider: []string{"k8s"},
			want:     []string{"k8s"},
		},
		{
			name:     "multiple providers",
			provider: []string{"k8s", "consul"},
			want:     []string{"k8s", "consul"},
		},
		{
			name:     "entries are trimmed and empties dropped",
			provider: []string{"  k8s ", "", "consul\t"},
			want:     []string{"k8s", "consul"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load("test", Flags{Providers: tt.provider})
			if err != nil {
				t.Fatalf("Load(\"test\", ...) returned error: %v", err)
			}
			if !reflect.DeepEqual(got.Providers, tt.want) {
				t.Errorf("Providers = %v, want %v", got.Providers, tt.want)
			}
		})
	}
}

func TestLoadNoProvidersConfigured(t *testing.T) {
	tests := []struct {
		name      string
		providers []string
	}{
		{name: "nil flag providers and empty preset", providers: nil},
		{name: "empty flag providers and empty preset", providers: []string{}},
		{name: "whitespace only providers", providers: []string{"   ", ""}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load("dev", Flags{Providers: tt.providers})
			if err == nil {
				t.Fatalf("Load(\"dev\", ...) succeeded, want error")
			}
			if err.Error() != "no providers configured" {
				t.Errorf("error = %q, want %q", err.Error(), "no providers configured")
			}
			if !reflect.DeepEqual(got, Config{}) {
				t.Errorf("Load(\"dev\", ...) = %+v, want zero Config", got)
			}
		})
	}
}

func TestLoadTrimsPushAppCodes(t *testing.T) {
	tests := []struct {
		name     string
		appCodes []string
		want     []string
	}{
		{
			name:     "entries are trimmed and empties dropped",
			appCodes: []string{" app-1 ", "", "\tapp-2\n"},
			want:     []string{"app-1", "app-2"},
		},
		{
			name:     "nil appcodes stays empty",
			appCodes: nil,
			want:     []string{},
		},
		{
			name:     "whitespace only appcodes stays empty",
			appCodes: []string{"  "},
			want:     []string{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load("test", Flags{Providers: []string{"k8s"}, PushAppCodes: tt.appCodes})
			if err != nil {
				t.Fatalf("Load(\"test\", ...) returned error: %v", err)
			}
			if !reflect.DeepEqual(got.PushAppCodes, tt.want) {
				t.Errorf("PushAppCodes = %v, want %v", got.PushAppCodes, tt.want)
			}
		})
	}
}

func TestLoadLeaderElectionTriState(t *testing.T) {
	tests := []struct {
		name string
		flag *bool
		want bool
	}{
		{name: "unset flag defaults to true", flag: nil, want: true},
		{name: "explicit true", flag: boolPtr(true), want: true},
		{name: "explicit false is honored", flag: boolPtr(false), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load("test", Flags{Providers: []string{"k8s"}, LeaderElection: tt.flag})
			if err != nil {
				t.Fatalf("Load(\"test\", ...) returned error: %v", err)
			}
			if got.EnableLeaderElection != tt.want {
				t.Errorf("EnableLeaderElection = %v, want %v", got.EnableLeaderElection, tt.want)
			}
		})
	}
}

// TestLoadInvalidEnv verifies unsupported environments fail with the exact
// legacy error.
func TestLoadInvalidEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{name: "unknown env", env: "bogus"},
		{name: "empty env", env: ""},
		{name: "prod abbreviation", env: "prod"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(tt.env, Flags{Providers: []string{"k8s"}})
			if err == nil {
				t.Fatalf("Load(%q, ...) succeeded, want error", tt.env)
			}
			if want := fmt.Sprintf("unsupported environment: %q", tt.env); err.Error() != want {
				t.Errorf("Load(%q, ...) error = %q, want %q", tt.env, err.Error(), want)
			}
			if !reflect.DeepEqual(got, Config{}) {
				t.Errorf("Load(%q, ...) = %+v, want zero Config", tt.env, got)
			}
		})
	}
}

// TestLoadTriStateExplicitZero verifies the tri-state pointer fields honor
// explicit zero values that the plain int fields cannot express: an
// explicit --log-level=0 (info) must not be replaced by the -1 default,
// and an explicit --push-interval=0 must not become 21600.
func TestLoadTriStateExplicitZero(t *testing.T) {
	flags := Flags{
		Providers:       []string{"k8s"},
		LogLevelPtr:     intPtr(0),
		PushIntervalPtr: intPtr(0),
	}
	got, err := Load("test", flags)
	if err != nil {
		t.Fatalf("Load(\"test\", ...) returned error: %v", err)
	}
	if got.LogLevel != 0 {
		t.Errorf("LogLevel = %d, want 0 (explicit zero honored)", got.LogLevel)
	}
	if got.PushAllInterval != 0 {
		t.Errorf("PushAllInterval = %d, want 0 (explicit zero honored)", got.PushAllInterval)
	}
}

// TestLoadTriStateExplicitFalse verifies the tri-state LogToStdPtr field
// honors an explicit --log-to-std=false instead of coercing it back to the
// default true.
func TestLoadTriStateExplicitFalse(t *testing.T) {
	got, err := Load("test", Flags{
		Providers:   []string{"k8s"},
		LogToStdPtr: boolPtr(false),
	})
	if err != nil {
		t.Fatalf("Load(\"test\", ...) returned error: %v", err)
	}
	if got.LogToStd {
		t.Errorf("LogToStd = true, want false (explicit false honored)")
	}
}

// TestLoadTriStateExplicitValues verifies non-zero tri-state values are
// used verbatim for every mapped flag.
func TestLoadTriStateExplicitValues(t *testing.T) {
	flags := Flags{
		Providers:       []string{"k8s"},
		LogSizePtr:      intPtr(5),
		LogBackupsPtr:   intPtr(3),
		LogAgePtr:       intPtr(14),
		LogToStdPtr:     boolPtr(true),
		PushIntervalPtr: intPtr(60),
	}
	got, err := Load("test", flags)
	if err != nil {
		t.Fatalf("Load(\"test\", ...) returned error: %v", err)
	}
	if got.LogSize != 5 {
		t.Errorf("LogSize = %d, want 5", got.LogSize)
	}
	if got.LogBackups != 3 {
		t.Errorf("LogBackups = %d, want 3", got.LogBackups)
	}
	if got.LogAge != 14 {
		t.Errorf("LogAge = %d, want 14", got.LogAge)
	}
	if !got.LogToStd {
		t.Errorf("LogToStd = false, want true")
	}
	if got.PushAllInterval != 60 {
		t.Errorf("PushAllInterval = %d, want 60", got.PushAllInterval)
	}
}

// TestLoadTriStateNilKeepsDefaults verifies nil tri-state pointers leave
// the plain-field/default resolution untouched (defaults still applied
// when no flag value is present).
func TestLoadTriStateNilKeepsDefaults(t *testing.T) {
	got, err := Load("test", Flags{Providers: []string{"k8s"}})
	if err != nil {
		t.Fatalf("Load(\"test\", ...) returned error: %v", err)
	}
	if got.LogSize != defaultLogSize {
		t.Errorf("LogSize = %d, want default %d", got.LogSize, defaultLogSize)
	}
	if got.LogLevel != defaultLogLevel {
		t.Errorf("LogLevel = %d, want default %d", got.LogLevel, defaultLogLevel)
	}
	if got.LogBackups != defaultLogBackups {
		t.Errorf("LogBackups = %d, want default %d", got.LogBackups, defaultLogBackups)
	}
	if got.LogAge != defaultLogAge {
		t.Errorf("LogAge = %d, want default %d", got.LogAge, defaultLogAge)
	}
	if !got.LogToStd {
		t.Errorf("LogToStd = false, want default true")
	}
	if got.PushAllInterval != defaultPushAllInterval {
		t.Errorf("PushAllInterval = %d, want default %d", got.PushAllInterval, defaultPushAllInterval)
	}
}

// TestLoadTriStateOverridesPlainField verifies a set tri-state pointer
// wins over a value supplied through the plain field.
func TestLoadTriStateOverridesPlainField(t *testing.T) {
	got, err := Load("test", Flags{
		Providers:   []string{"k8s"},
		LogLevel:    1,
		LogLevelPtr: intPtr(0),
	})
	if err != nil {
		t.Fatalf("Load(\"test\", ...) returned error: %v", err)
	}
	if got.LogLevel != 0 {
		t.Errorf("LogLevel = %d, want 0 (tri-state field wins)", got.LogLevel)
	}
}

// intPtr boxes an int value for the tri-state Flags fields.
// (boolPtr is already declared at the top of this file.)
func intPtr(v int) *int { return &v }

func TestLoadCaseInsensitiveEnv(t *testing.T) {
	tests := []struct {
		env        string
		wantPreset Endpoints
	}{
		{env: "Test", wantPreset: testPreset()},
		{env: "DEV", wantPreset: devPreset()},
		{env: "Product", wantPreset: prodPreset()},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.env, func(t *testing.T) {
			got, err := Load(tt.env, Flags{Providers: []string{"k8s"}})
			if err != nil {
				t.Fatalf("Load(%q, ...) returned error: %v", tt.env, err)
			}
			if !reflect.DeepEqual(got.Endpoints, tt.wantPreset) {
				t.Errorf("Load(%q, ...).Endpoints = %+v, want %+v", tt.env, got.Endpoints, tt.wantPreset)
			}
		})
	}
}

func TestLoadPropagatesEnvVerbatim(t *testing.T) {
	// The env name selects the preset case-insensitively but is carried
	// through Config.Env verbatim so notice wiring sees the original value.
	got, err := Load("Product", Flags{Providers: []string{"k8s"}})
	if err != nil {
		t.Fatalf("Load(\"Product\", ...) returned error: %v", err)
	}
	if got.Env != "Product" {
		t.Errorf("Env = %q, want %q", got.Env, "Product")
	}
}

func TestLoadErrorKeepsEnvEmpty(t *testing.T) {
	got, err := Load("bogus", Flags{Providers: []string{"k8s"}})
	if err == nil {
		t.Fatal("Load(\"bogus\", ...) succeeded, want error")
	}
	if got.Env != "" {
		t.Errorf("Env = %q, want empty on error", got.Env)
	}
}
