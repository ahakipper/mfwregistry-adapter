package internal

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"

	"spotter/internal/composition"
	infraconfig "spotter/internal/infra/config"
	"spotter/internal/testkit/etcdmock"
	"spotter/internal/testkit/fakes"
	"spotter/pkg/worker"
)

// This file pins the AUDIT-D-1 wiring: the composition root (composition.Build)
// → server constructor (NewServerFromDeps) → elector constructor
// (worker.NewElectorWithDeps) → candidate notify chain that pages EMERGENCY
// on campaign failure. Before this test the chain had zero coverage: every
// server test built a &Server{} literal with its own notifier, bypassing the
// wiring that threads rt.Notifier into the elector — so the E3-class
// regression (a nil notifier silently severing EMERGENCY paging) could not
// fail any test.
//
// The technique mirrors tests/e2e/elector_e2e_test.go's lease-revocation:
// NewElectorWithDeps grants exactly one lease (the candidate's session lease)
// during construction; an independent administrative client revokes that
// lease BEFORE ElectWait runs, so the candidate's first Campaign txn fails
// with a genuine server error ("etcdserver: requested lease not found", not
// context.DeadlineExceeded) — exactly the branch election.Campaign pages on.
// The test stays in the unit tier by using the embedded etcdmock server
// (loopback, dynamic ports) instead of the real network.

// wiringCampaignKey is the etcd prefix key the wiring test campaigns on
// (unique to this test; never a production campaign key).
const wiringCampaignKey = "/spotter-wiring-test/election"

// wiringNoticeTimeout bounds waiting for the campaign-failure page: the first
// Campaign against the revoked lease pages immediately; the e2e tier uses 15s
// against the same mechanics, and this bound keeps generous headroom.
const wiringNoticeTimeout = 15 * time.Second

// TestNewServerFromDepsThreadsNotifierToElector proves the real constructor
// chain: composition.Build injects the fakes.FakeNotifier as
// runtime.Notifier, NewServerFromDeps passes it (with the config's etcd
// endpoints and campaign key) to worker.NewElectorWithDeps, and the resulting
// elector's candidate pages "Candidate server node election failed" through
// THAT notifier when its campaign fails. The lease-revocation failure is
// injected out of band before ElectWait starts, so the ordering is enforced
// rather than hoped for.
func TestNewServerFromDepsThreadsNotifierToElector(t *testing.T) {
	server, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("etcdmock.Start() error = %v, want nil", err)
	}
	defer server.Close()

	// The composition root exactly as cmd/adapter.go builds it, except the
	// notifier (and logger) are test doubles injected through Deps — the
	// documented override seam. The config is a minimal resolved Config with
	// the etcdmock endpoints and campaign key NewServerFromDeps reads; a
	// provider must be configured or the constructor rejects the runtime.
	notifier := &fakes.FakeNotifier{}
	rt, err := composition.Build(wiringConfig(server), composition.Deps{
		Logger:   &fakes.FakeLogger{},
		Notifier: notifier,
	})
	if err != nil {
		t.Fatalf("composition.Build() error = %v, want nil", err)
	}

	srv, err := NewServerFromDeps(rt)
	if err != nil {
		t.Fatalf("NewServerFromDeps() error = %v, want nil", err)
	}
	defer srv.Stop()

	// The server owns the elector context; drive the elector through the
	// same public surface Run uses (ElectWait on the leader channel).
	elector, ok := srv.elector.(worker.Elector)
	if !ok {
		t.Fatalf("server elector = %T, want a worker.Elector", srv.elector)
	}
	leaderCh := make(chan bool, 16)

	// An independent administrative client revokes the candidate's session
	// lease so the first Campaign txn fails with a server error instead of
	// winning the key (same technique as the e2e tier).
	admin, err := clientv3.New(clientv3.Config{Endpoints: server.ClientEndpoints()})
	if err != nil {
		t.Fatalf("clientv3.New() error = %v, want nil", err)
	}
	defer func() { _ = admin.Close() }()

	listCtx, listCancel := context.WithTimeout(context.Background(), 5*time.Second)
	leases, err := admin.Lease.Leases(listCtx)
	listCancel()
	if err != nil {
		t.Fatalf("Lease.Leases() error = %v, want nil", err)
	}
	if len(leases.Leases) != 1 {
		t.Fatalf("server leases = %d, want exactly the candidate's session lease", len(leases.Leases))
	}

	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, revokeErr := admin.Lease.Revoke(revokeCtx, leases.Leases[0].ID)
	revokeCancel()
	if revokeErr != nil {
		t.Fatalf("Lease.Revoke() error = %v, want nil", revokeErr)
	}

	// Only now start the loop; its first Campaign runs against the
	// already-revoked lease and pages through the injected notifier.
	go elector.ElectWait(leaderCh)

	// The EMERGENCY page must arrive through the chain Build →
	// NewServerFromDeps → NewElectorWithDeps → candidate.notify. Cancelling
	// the elector context (srv.Stop's stopElectorFunc) bounds the retry loop.
	deadline := time.Now().Add(wiringNoticeTimeout)
	for time.Now().Before(deadline) {
		for _, notification := range notifier.Notifications() {
			if notification.Title == "Candidate server node election failed" {
				srv.Stop()
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	srv.Stop()
	t.Fatalf("no campaign-failure notice within %s through the wired notifier; notifications = %#v",
		wiringNoticeTimeout, notifier.Notifications())
}

// TestNewServerFromDepsRejectsProviderlessRuntime pins the constructor guard
// the wiring test relies on: a runtime whose config declares no providers is
// rejected up front (the runtime the composition root hands over in
// production always carries the flag-resolved provider list).
func TestNewServerFromDepsRejectsProviderlessRuntime(t *testing.T) {
	cfg := wiringConfig(nil)
	cfg.Providers = nil
	rt, err := composition.Build(cfg, composition.Deps{
		Logger:   &fakes.FakeLogger{},
		Notifier: &fakes.FakeNotifier{},
	})
	if err != nil {
		t.Fatalf("composition.Build() error = %v, want nil", err)
	}
	if _, err := NewServerFromDeps(rt); err == nil {
		t.Fatal("NewServerFromDeps() error = nil, want error for a providerless runtime")
	}
}

// wiringConfig builds the minimal resolved Config NewServerFromDeps consumes:
// etcd endpoints/campaign key (from the embedded server), one provider so the
// provider guard passes, and no log file so the injected logger override is
// the only logging (Build would otherwise construct a real file logger — the
// override skips that, exactly the documented Deps semantics).
//
// server may be nil for tests that never construct the elector (only the
// guard test); its endpoints are the sole consumer of the argument.
func wiringConfig(server *etcdmock.Server) infraconfig.Config {
	var endpoints []string
	if server != nil {
		endpoints = server.ClientEndpoints()
	}
	return infraconfig.Config{
		Endpoints: infraconfig.Endpoints{
			EtcdEndpoints:   endpoints,
			LockCampaignKey: wiringCampaignKey,
		},
		Env:               "test",
		Providers:         []string{"k8s"},
		PushAllInterval:   21600,
		MetricsAddr:       "127.0.0.1:0",
		DisablePushWorker: true,
	}
}
