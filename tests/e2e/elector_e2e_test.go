//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"

	"spotter/internal/testkit/etcdmock"
	"spotter/internal/testkit/fakes"
	"spotter/pkg/worker"
)

// The elector e2e drives the REAL etcd-backed election path that no unit
// tier can reach (pkg/distribute/election has no in-package tests): two
// electors built through worker.NewElectorWithDeps (real clientv3 client,
// real concurrency.Election sessions, real 2s leader poll) campaign on one
// key against an embedded etcd (etcdmock, loopback, dynamic ports).
//
// Timing model (all loopback, measured on a dev machine):
//   - embedded etcd boot: ~0.6s
//   - initial election: <1s (single member raft campaign, no contender keys)
//   - lease TTL: NewElectionSession grants TTL 0, the server clamps it to
//     MinLeaseTTL = ceil(1.5 * ElectionMs / 1000) = 2s (embed defaults
//     TickMs=100, ElectionMs=1000)
//   - failover after leader Stop: measured ~3s (leader client close ->
//     keepalive stream dies -> lease lapses -> etcd's 500ms lease loop
//     revokes it -> leader key deleted -> follower's parked Campaign
//     returns -> next 2s poll observes true)
//
// Every wait below is bounded with generous deadlines on top of those.

const (
	// e2eCampaignKey is the shared etcd prefix key both electors campaign
	// on (unique to the e2e tier; never a production campaign key).
	e2eCampaignKey = "/spotter-e2e/election"

	// e2eElectTimeout bounds the initial election: campaign 10s timeout
	// plus the 2s leader poll bound the window; 15s adds headroom.
	e2eElectTimeout = 15 * time.Second

	// e2eSettleWindow proves mutual exclusion: the follower channel is
	// drained longer than one 2s leader poll, so a spurious true (double
	// election) would surface inside the window.
	e2eSettleWindow = 2500 * time.Millisecond

	// e2eFailoverTimeout bounds leadership reacquisition after the leader
	// stops: 2s poll + 2s clamped lease TTL + 500ms lease revoke cadence,
	// measured ~3s end to end; 30s keeps the bound generous.
	e2eFailoverTimeout = 30 * time.Second

	// e2eNoticeTimeout bounds waiting for the campaign-failure notice.
	e2eNoticeTimeout = 15 * time.Second
)

// TestE2EElectorElectsSingleLeaderAndFailsOver proves, against a real
// embedded etcd: (a) two electors on one campaign key elect exactly one
// leader, (b) the follower never receives true while the leader is up
// (E3's on-change dispatch still delivers the follower's initial false,
// which is expected), (c) stopping the leader — Stop closes the etcd
// client the elector owns, ending the lease keepalive — fails over to the
// follower, and (d) the dead elector forwards nothing further.
func TestE2EElectorElectsSingleLeaderAndFailsOver(t *testing.T) {
	bootedAt := time.Now()
	server, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("etcdmock.Start() error = %v, want nil", err)
	}
	defer server.Close()
	bootDuration := time.Since(bootedAt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two independent electors: separate leader channels, separate fake
	// loggers, one shared campaign key, both real etcd clients.
	firstCh := make(chan bool, 16)
	firstLogger := &fakes.FakeLogger{}
	first := newE2EElector(t, ctx, server, firstCh, firstLogger)

	secondCh := make(chan bool, 16)
	secondLogger := &fakes.FakeLogger{}
	second := newE2EElector(t, ctx, server, secondCh, secondLogger)

	// --- (a) Exactly one leader ------------------------------------------
	// Which elector wins is scheduler/raft timing, so identify the leader
	// dynamically and stop THAT one in the failover phase below.
	electedAt := time.Now()
	go first.ElectWait(firstCh)
	go second.ElectWait(secondCh)

	leader, _, leaderCh, loserCh := awaitExactlyOneTrue(t, first, second, firstCh, secondCh, e2eElectTimeout)
	// The winner's first observation is the true already consumed above;
	// the loser's first observation dispatches false (E3 leaderState
	// dispatches the first observation regardless of value).
	drainInitialDispatch(t, loserCh, false, "follower")
	t.Logf("embedded etcd boot %s; exactly one leader elected in %s",
		bootDuration, time.Since(electedAt))

	// --- (b) Mutual exclusion --------------------------------------------
	// While the leader is alive the follower channel must never receive
	// true. (A false may arrive as the follower's first observation; the
	// candidate's observed state is never reset across its re-campaign
	// cycle, so repeated falses are deduplicated — only a genuine
	// leadership change would dispatch again.)
	assertNoTrueDispatch(t, loserCh, e2eSettleWindow, "follower channel while the leader is up")

	// --- (c) Failover ----------------------------------------------------
	// Stop the leader: Stop closes the owned etcd client, which kills the
	// lease keepalive stream, so the ~2s clamped lease lapses and etcd
	// deletes the leader key; the follower's parked Campaign then wins.
	//
	// Measured: ~3-4.5s (leader client close -> lease lapses -> etcd's
	// 500ms lease loop revokes it -> leader key deleted -> follower's
	// Campaign returns -> its next 2s poll observes true).
	stoppedAt := time.Now()
	leader.Stop()

	// The follower may dispatch additional false values while its Wait
	// loop re-campaigns; the failover contract is the TRUE that must
	// arrive within the bound.
	deadline := time.After(e2eFailoverTimeout)
	for {
		select {
		case got := <-loserCh:
			if got {
				t.Logf("failover after leader Stop: %s", time.Since(stoppedAt))
				goto failoverDone
			}
			// A false during the window is the follower's re-campaign
			// chatter; keep waiting for the true.
		case <-deadline:
			t.Fatalf("no failover within %s after the leader stopped", e2eFailoverTimeout)
		}
	}
failoverDone:

	// --- (d) The dead elector stays silent -------------------------------
	// Its stopped flag suppresses forwarding: no true may arrive on the
	// dead leader's channel after the failover.
	assertNoTrueDispatch(t, leaderCh, e2eSettleWindow, "dead leader channel after failover")
}

// TestE2EElectorCampaignFailureNotifies asserts the campaign-failure page
// through the injected notifier (F7/E3 seam) on a REAL embedded etcd.
//
// Why lease revocation instead of a refused endpoint: the etcd client dials
// with WaitForReady and the candidate's session Grant (NewCandidateWithDeps
// → client.Grant) parks indefinitely against a dead endpoint, which would
// hang the test before Campaign ever runs. Revoking the candidate's just
// granted lease instead makes the very first Campaign txn fail with a
// genuine server error ("etcdserver: requested lease not found" — not
// context.DeadlineExceeded), which is exactly the branch
// election.Campaign pages on:
//
//	if errors.Is(err, context.DeadlineExceeded) { return err }  // no page
//	c.notifier.Notify("Candidate server node election failed", ...)
//
// The unreachable-endpoint contract at the constructor level (fail-fast
// MemberList probe) is asserted first, bounded by the probe's own 5s
// deadline.
func TestE2EElectorCampaignFailureNotifies(t *testing.T) {
	// --- Constructor fail-fast against a refused endpoint -----------------
	// 127.0.0.1:1 is a loopback port nothing listens on: connections are
	// refused immediately (offline, no corporate endpoints). The lazy
	// client's MemberList probe fails fast and NewElectorWithDeps returns
	// an error well inside the 8s guard (same shape as the unit-tier
	// fail-fast test, but with the real network stack).
	constructorDone := make(chan error, 1)
	go func() {
		_, err := worker.NewElectorWithDeps(
			context.Background(),
			make(chan bool, 1),
			[]string{"127.0.0.1:1"},
			"", "", "",
			e2eCampaignKey,
			&fakes.FakeLogger{},
			nil,
		)
		constructorDone <- err
	}()
	select {
	case err := <-constructorDone:
		if err == nil {
			t.Fatal("NewElectorWithDeps() error = nil against a refused endpoint, want a probe error")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("NewElectorWithDeps() did not fail fast within 8s for a refused etcd endpoint")
	}

	// --- Campaign failure pages through the injected notifier -------------
	server, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("etcdmock.Start() error = %v, want nil", err)
	}
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaderCh := make(chan bool, 16)
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}

	// A real elector over the embedded server; construction grants exactly
	// one lease (the candidate's session lease).
	elector, err := worker.NewElectorWithDeps(
		ctx,
		leaderCh,
		server.ClientEndpoints(),
		"", "", "",
		e2eCampaignKey,
		logger,
		notifier,
	)
	if err != nil {
		t.Fatalf("worker.NewElectorWithDeps() error = %v, want nil", err)
	}

	// An independent administrative client revokes that lease so the first
	// Campaign txn fails with a server error ("requested lease not
	// found") instead of DeadlineExceeded.
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

	// Revoke the lease BEFORE spawning ElectWait, so the ordering is
	// enforced rather than hoped for: the revoke completes (this line
	// returns) before the candidate's first Campaign txn can commit, and
	// that txn then fails with a server error ("requested lease not
	// found") instead of DeadlineExceeded. Revoke-after-spawn would race
	// the first Campaign: a Campaign that committed before the revoke
	// would win the key and never page.
	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, revokeErr := admin.Lease.Revoke(revokeCtx, leases.Leases[0].ID)
	revokeCancel()
	if revokeErr != nil {
		t.Fatalf("Lease.Revoke() error = %v, want nil", revokeErr)
	}

	// Only now start the loop; its first Campaign runs against the
	// already-revoked lease.
	go elector.ElectWait(leaderCh)

	// The first Campaign against the revoked lease pages at once; the
	// ElectWait loop then retries, so the notice must arrive quickly.
	// Cancelling the elector ctx bounds the loop (the candidate's Wait
	// exits, ending the re-campaign churn).
	deadline := time.Now().Add(e2eNoticeTimeout)
	for time.Now().Before(deadline) {
		for _, notification := range notifier.Notifications() {
			if notification.Title == "Candidate server node election failed" {
				cancel()
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	t.Fatalf("no campaign-failure notice within %s; notifications = %#v",
		e2eNoticeTimeout, notifier.Notifications())
}

// --- helpers ---------------------------------------------------------------

// awaitExactlyOneTrue waits until exactly one of the two channels receives
// true and returns (leader, loser, leaderChannel, loserChannel). Receiving
// true on both within the window fails the test (double election). The
// winner's first true dispatch is consumed here; the loser's initial false
// stays queued for the caller to drain.
func awaitExactlyOneTrue(t *testing.T, first, second worker.Elector, firstCh, secondCh chan bool, timeout time.Duration) (leader, loser worker.Elector, leaderCh, loserCh chan bool) {
	t.Helper()
	leaderChosen := ""
	deadline := time.After(timeout)
	for leaderChosen == "" {
		select {
		case got := <-firstCh:
			if got {
				leaderChosen = "first"
			}
		case got := <-secondCh:
			if got {
				leaderChosen = "second"
			}
		case <-deadline:
			t.Fatalf("no leader elected within %s", timeout)
		}
	}
	// A true on the other channel now would be a double election; one
	// settle window (longer than the 2s poll) makes it surface.
	assertNoTrueDispatch(t, other(leaderChosen, firstCh, secondCh), e2eSettleWindow, "other elector channel")
	if leaderChosen == "first" {
		return first, second, firstCh, secondCh
	}
	return second, first, secondCh, firstCh
}

func other(chosen string, first, second chan bool) chan bool {
	if chosen == "first" {
		return second
	}
	return first
}

// drainInitialDispatch asserts the channel's next dispatch matches want
// when one arrives soon. The follower's initial false can lag far behind
// the winner's true (its Wait loop only starts polling after its first
// Campaign, which parks up to the 10s campaign timeout while the winner's
// key exists — observed ~6.5s total), so a missing dispatch within the
// grace window is not a failure — the invariant that matters (the follower
// never dispatches true) is enforced by assertNoTrueDispatch. A true
// arriving here IS a failure (double election caught early).
func drainInitialDispatch(t *testing.T, ch chan bool, want bool, who string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("%s initial dispatch = %v, want %v", who, got, want)
		}
	case <-time.After(2 * time.Second):
		// No initial dispatch within the grace window; proceed — the
		// no-true assertion below enforces the actual contract.
	}
}

// assertNoTrueDispatch asserts the channel receives no true within the
// window; false values are drained and ignored.
func assertNoTrueDispatch(t *testing.T, ch chan bool, window time.Duration, what string) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case got := <-ch:
			if got {
				t.Fatalf("%s received true, want no true within %s", what, window)
			}
		case <-deadline:
			return
		}
	}
}

// newE2EElector builds a REAL elector (real clientv3 client via
// worker.NewElectorWithDeps, real candidate) against the embedded server.
// Only the logger is a fake.
func newE2EElector(t *testing.T, ctx context.Context, server *etcdmock.Server, leaderCh chan bool, logger *fakes.FakeLogger) worker.Elector {
	t.Helper()
	elector, err := worker.NewElectorWithDeps(
		ctx,
		leaderCh,
		server.ClientEndpoints(),
		"", "", "",
		e2eCampaignKey,
		logger,
		nil,
	)
	if err != nil {
		t.Fatalf("worker.NewElectorWithDeps() error = %v, want nil", err)
	}
	return elector
}
