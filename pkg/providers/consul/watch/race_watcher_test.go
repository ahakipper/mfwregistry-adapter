package watch

import (
	"context"
	"testing"
	"time"
)

func TestWatcherPool_Watch(t *testing.T) {
	w1 := testBuildWatch(addr1, t)
	w2 := testBuildWatch(addr2, t)

	rw := &RaceWatcher{}
	rw.AddWatcher(w1)
	rw.AddWatcher(w2)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*20)
	closedCh, err := rw.Watch(ctx)
	if err != nil {
		t.Error(err.Error())
		return
	}
	<-closedCh
	t.Log("watch pool exited")
}
