package watch

import (
    "context"
    "fmt"
    "testing"
    "time"
)

var (
    addr1 = "10.72.73.172:8520"
    addr2 = "10.72.73.173:8520"
)

func TestWatcher_Watch(t *testing.T) {
    w := testBuildWatch(addr1, t)

    ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
    err := w.Watch(ctx)
    if err != nil {
        t.Error(err.Error())
        return
    }
    t.Log("unwatched")
}

func testBuildWatch(addr string, t *testing.T) *Watcher {
    h := func(idx uint64, data interface{}) {
        fmt.Printf("event data: %v\n", data)
    }
    w, err := NewWatcher(addr, h)
    if err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
    return w
}
