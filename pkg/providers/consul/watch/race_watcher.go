package watch

import (
    "context"
    "github.com/pkg/errors"
    "sync"
    "time"
)

type RaceWatcher struct {
    watchers []*Watcher
}

func (rw *RaceWatcher) AddWatcher(w *Watcher) {
    rw.watchers = append(rw.watchers, w)
}

func (rw *RaceWatcher) Watch(ctx context.Context) (chan struct{}, error) {
    if len(rw.watchers) == 0 {
        return nil, errors.New("no watchers")
    }

    ch := make(chan struct{})
    go func() {
        defer close(ch)
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }

            unwatchCh, err := rw.raceWatch(ctx)
            if err == nil && unwatchCh != nil {
                <-unwatchCh
            }

            time.Sleep(time.Second)
        }
    }()
    return ch, nil
}

func (rw *RaceWatcher) raceWatch(ctx context.Context) (chan struct{}, error) {
    type competitor struct {
        w  *Watcher
        ch chan struct{}

        cancelFn context.CancelFunc
    }

    cs := make(map[int]*competitor)

    wg := sync.WaitGroup{}

    winnerCh := make(chan int)
    for i, watcher := range rw.watchers {
        wg.Add(1)

        watcherCtx, cancelFn := context.WithCancel(ctx)
        c := &competitor{
            w:        watcher,
            ch:       make(chan struct{}),
            cancelFn: cancelFn,
        }
        cs[i] = c

        go func(i int) {
            defer wg.Done()
            if err := c.w.Watch(watcherCtx); err == nil {
                winnerCh <- i
            }
        }(i)
    }

    allDone := make(chan struct{})
    go func() {
        wg.Wait()
        close(allDone)
    }()

    select {
    case winnerIndex := <-winnerCh:
        // cancel other competitors
        for i, c := range cs {
            if i != winnerIndex {
                c.cancelFn()
            }
        }

        return cs[winnerIndex].ch, nil
    case <-allDone:
        return nil, errors.New("no watcher acquired lock")
    }
}
