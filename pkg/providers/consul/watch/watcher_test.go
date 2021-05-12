package watch

import (
    "context"
    "fmt"
    "testing"
    "time"
)

var (
    addr1 = "172.16.129.2:8500"
    addr2 = "172.16.129.3:8500"
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
