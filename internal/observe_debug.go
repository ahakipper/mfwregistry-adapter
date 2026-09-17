package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	domaininstance "spotter/internal/domain/instance"
	v2 "spotter/pkg/beehive/service/v2"
)

const observeEventCapacity = 32768

// observeDebugEvent is one test-only Spotter middle-plane boundary. It is
// emitted after K8s conversion/filtering and before the worker calls sinks.
type observeDebugEvent struct {
	Sequence         uint64 `json:"sequence"`
	Boundary         string `json:"boundary"`
	Operation        string `json:"operation"`
	Origin           string `json:"origin"`
	TriggerAt        string `json:"triggerAt"`
	ObservedAt       string `json:"observedAt"`
	InstanceID       string `json:"instanceId"`
	AppCode          string `json:"appCode"`
	SourceKey        string `json:"sourceKey"`
	SourceCluster    string `json:"sourceCluster"`
	Reversion        int64  `json:"reversion"`
	Status           int32  `json:"status"`
	CanonicalPayload string `json:"canonicalPayload"`
}

type observeEventBatch struct {
	Events           []observeDebugEvent `json:"events"`
	EarliestSequence uint64              `json:"earliestSequence"`
	LatestSequence   uint64              `json:"latestSequence"`
	Gap              bool                `json:"gap"`
}

type observeEventRecorder struct {
	mu       sync.Mutex
	capacity int
	next     uint64
	events   []observeDebugEvent
	notify   chan struct{}
}

func newObserveEventRecorder(capacity int) *observeEventRecorder {
	if capacity < 1 {
		capacity = 1
	}
	return &observeEventRecorder{capacity: capacity, notify: make(chan struct{})}
}

func (r *observeEventRecorder) record(triggerTime int64, operation, origin string, ins *v2.Instance) {
	if r == nil || ins == nil {
		return
	}
	now := time.Now().UTC()
	triggerAt := ""
	if triggerTime > 0 {
		triggerAt = time.Unix(0, triggerTime).UTC().Format(time.RFC3339Nano)
	}
	r.mu.Lock()
	r.next++
	event := observeDebugEvent{
		Sequence: r.next, Boundary: "provider-output/pre-worker", Operation: operation, Origin: origin,
		TriggerAt: triggerAt, ObservedAt: now.Format(time.RFC3339Nano),
		InstanceID: ins.InstanceId, AppCode: ins.AppCode, SourceKey: ins.SourceKey,
		SourceCluster: ins.SourceCluster, Reversion: ins.Reversion, Status: ins.Status,
		CanonicalPayload: domaininstance.CanonicalPayload(ins),
	}
	r.events = append(r.events, event)
	if len(r.events) > r.capacity {
		r.events = append([]observeDebugEvent(nil), r.events[len(r.events)-r.capacity:]...)
	}
	close(r.notify)
	r.notify = make(chan struct{})
	r.mu.Unlock()
}

func (r *observeEventRecorder) snapshotAfter(after uint64) (observeEventBatch, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	batch := observeEventBatch{LatestSequence: r.next}
	if len(r.events) > 0 {
		batch.EarliestSequence = r.events[0].Sequence
		if after+1 < batch.EarliestSequence {
			batch.Gap = true
		}
		for _, event := range r.events {
			if event.Sequence > after {
				batch.Events = append(batch.Events, event)
			}
		}
	}
	return batch, r.notify
}

func (r *observeEventRecorder) latestSequence() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

func (r *observeEventRecorder) waitAfter(ctx context.Context, after uint64, wait time.Duration) observeEventBatch {
	for {
		batch, notify := r.snapshotAfter(after)
		if len(batch.Events) > 0 || wait <= 0 {
			return batch
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			batch, _ = r.snapshotAfter(after)
			return batch
		case <-timer.C:
			batch, _ = r.snapshotAfter(after)
			return batch
		case <-notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

func (s *Server) handleObserveDebugEvents(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	after, err := strconv.ParseUint(req.URL.Query().Get("after"), 10, 64)
	if err != nil && req.URL.Query().Get("after") != "" {
		http.Error(w, "invalid after sequence", http.StatusBadRequest)
		return
	}
	wait := 5 * time.Second
	if raw := req.URL.Query().Get("wait"); raw != "" {
		wait, err = time.ParseDuration(raw)
		if err != nil || wait < 0 || wait > 10*time.Second {
			http.Error(w, "invalid wait duration", http.StatusBadRequest)
			return
		}
	}
	if s.observeDebug == nil {
		http.Error(w, "observe debug recorder disabled", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.observeDebug.waitAfter(req.Context(), after, wait))
}

var observeDebugHandlersOnce sync.Once
var observeDebugServer atomic.Pointer[Server]

func registerObserveDebugHandlers(s *Server) {
	observeDebugServer.Store(s)
	observeDebugHandlersOnce.Do(func() {
		http.HandleFunc("/debug/spotter/k8s", func(w http.ResponseWriter, req *http.Request) {
			if current := observeDebugServer.Load(); current != nil {
				current.handleObserveDebugSnapshot(w, req)
				return
			}
			http.NotFound(w, req)
		})
		http.HandleFunc("/debug/spotter/events", func(w http.ResponseWriter, req *http.Request) {
			if current := observeDebugServer.Load(); current != nil {
				current.handleObserveDebugEvents(w, req)
				return
			}
			http.NotFound(w, req)
		})
	})
}
