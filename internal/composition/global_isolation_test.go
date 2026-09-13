package composition

import (
	"reflect"
	"testing"

	legacyconfig "spotter/config"
	infraconfig "spotter/internal/infra/config"
	"spotter/internal/infra/legacycompat"
	"spotter/internal/testkit/fakes"
)

// TestCompositionBuildsIndependentRuntimesWithoutGlobalMutation protects the
// composition boundary from hidden process state. Building one runtime must
// not overwrite package globals or make a second runtime observe the first
// runtime's environment and collaborators.
func TestCompositionBuildsIndependentRuntimesWithoutGlobalMutation(t *testing.T) {
	oldEndpoints := append([]string(nil), legacyconfig.EtcdEndpoints...)
	oldCampaign := legacyconfig.LockCampaignKey
	oldPushAppCodes := append([]string(nil), legacyconfig.PushAppCodes...)
	t.Cleanup(func() {
		legacyconfig.EtcdEndpoints = oldEndpoints
		legacyconfig.LockCampaignKey = oldCampaign
		legacyconfig.PushAppCodes = oldPushAppCodes
	})
	legacyconfig.EtcdEndpoints = []string{"legacy:2379"}
	legacyconfig.LockCampaignKey = "/legacy/sentinel"
	legacyconfig.PushAppCodes = []string{"legacy-app"}

	legacycompat.ResetAccessCounts()
	t.Cleanup(legacycompat.ResetAccessCounts)

	firstLogger := &fakes.FakeLogger{}
	firstNotifier := &fakes.FakeNotifier{}
	firstConfig := infraconfig.Config{Env: "test", Providers: []string{"k8s"}}
	first, err := Build(firstConfig, Deps{Logger: firstLogger, Notifier: firstNotifier})
	if err != nil {
		t.Fatalf("Build(first) error = %v", err)
	}

	secondLogger := &fakes.FakeLogger{}
	secondNotifier := &fakes.FakeNotifier{}
	secondConfig := infraconfig.Config{Env: "product", Providers: []string{"consul"}}
	second, err := Build(secondConfig, Deps{Logger: secondLogger, Notifier: secondNotifier})
	if err != nil {
		t.Fatalf("Build(second) error = %v", err)
	}

	if first.Config.Env != "test" || second.Config.Env != "product" {
		t.Fatalf("runtime environments = (%q, %q), want (test, product)", first.Config.Env, second.Config.Env)
	}
	if !reflect.DeepEqual(first.Config, firstConfig) || !reflect.DeepEqual(second.Config, secondConfig) {
		t.Fatalf("runtime configs were not preserved independently: first=%+v second=%+v", first.Config, second.Config)
	}
	if first.Logger != firstLogger || second.Logger != secondLogger || first.Logger == second.Logger {
		t.Fatal("runtime loggers are not independent injected collaborators")
	}
	if first.Notifier != firstNotifier || second.Notifier != secondNotifier || first.Notifier == second.Notifier {
		t.Fatal("runtime notifiers are not independent injected collaborators")
	}

	if !reflect.DeepEqual(legacyconfig.EtcdEndpoints, []string{"legacy:2379"}) {
		t.Fatalf("legacy EtcdEndpoints mutated: %v", legacyconfig.EtcdEndpoints)
	}
	if legacyconfig.LockCampaignKey != "/legacy/sentinel" {
		t.Fatalf("legacy LockCampaignKey mutated: %q", legacyconfig.LockCampaignKey)
	}
	if !reflect.DeepEqual(legacyconfig.PushAppCodes, []string{"legacy-app"}) {
		t.Fatalf("legacy PushAppCodes mutated: %v", legacyconfig.PushAppCodes)
	}
	if counts := legacycompat.AccessCountsSnapshot(); counts.Reads != 0 || counts.Writes != 0 {
		t.Fatalf("composition.Build touched legacy compatibility boundary: %+v", counts)
	}
}
