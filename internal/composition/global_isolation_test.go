package composition

import (
	"reflect"
	"testing"

	infraconfig "spotter/internal/infra/config"
	"spotter/internal/testkit/fakes"
)

// TestCompositionBuildsIndependentRuntimes protects the composition boundary
// from hidden process state. Building one runtime must not make a second
// runtime observe the first runtime's configuration or collaborators.
func TestCompositionBuildsIndependentRuntimes(t *testing.T) {
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
}
