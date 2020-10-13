package worker

import (
    "context"
    "github.com/coreos/etcd/clientv3"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/distribute/election"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/etcd"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/log"
    "time"
)

// Elector is used for master-slave election
type Elector interface {
    // Start the distribute node
    // init distributed related work
    ElectWait()

    // Stop represent node exit
    Stop()
}

// worker implement Worker interface
type ElectWorker struct {
    ctx context.Context

    // candidate use for distributed election
    candidate election.Candidate

    etcdclient *clientv3.Client

    stopped bool
}

func NewElector(ctx context.Context, leaderChCh chan bool) (Elector, error) {
    etcdclient, err := etcd.NewEtcdClient()
    if err != nil {
        return nil, err
    }
    candite, err := election.NewCandidate(ctx, etcdclient)
    if err != nil {
        return nil, err
    }
    ew := &ElectWorker{
        ctx:        ctx,
        candidate:  candite,
        etcdclient: etcdclient,
    }
    ew.setLeaderChangeNotifyCall(leaderChCh)
    return ew, nil
}

// ElectWait will perform the behavior of electing the leader. It will always block and notify the relevant channel
// of leader changes
func (w *ElectWorker) ElectWait() {
    // start election campaign
    w.candidate.Campaign(10 * time.Second)

    go w.syncStoppedState()

    // waiting for loop election
    w.candidate.Wait()
}

func (w *ElectWorker) setLeaderChangeNotifyCall(ch chan bool) {
    var callback election.LeaderChangeFunc = func(isLeader bool) {
        // send leader changes to the channel if the elector is not stopped
        if !w.stopped {
            // pp.Println("leader change func: ", isLeader, "stoppped", w.stopped, &ch)
            ch <- isLeader
        }
    }
    w.candidate.AddObserveCallFunc(callback)
}

// Stop the worker, if the worker`s identity is leader
func (w *ElectWorker) Stop() {
    w.stopped = true
    log.Info("distribute worker stop")
}

func (w *ElectWorker) syncStoppedState() {
    for {
        select {
        case <-w.ctx.Done():
            w.stopped = true
        }
    }
}
