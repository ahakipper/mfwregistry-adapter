// Package fakes provides concurrency-safe implementations of application ports for tests.
package fakes

import (
	"context"
	"fmt"
	"sync"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

const (
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// LogEntry is one captured log call.
type LogEntry struct {
	Level   string
	Message string
}

// FakeLogger records log calls in order.
type FakeLogger struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (l *FakeLogger) Info(args ...interface{}) {
	l.add(LevelInfo, fmt.Sprint(args...))
}

func (l *FakeLogger) Infof(format string, args ...interface{}) {
	l.add(LevelInfo, fmt.Sprintf(format, args...))
}

func (l *FakeLogger) Warn(args ...interface{}) {
	l.add(LevelWarn, fmt.Sprint(args...))
}

func (l *FakeLogger) Warnf(format string, args ...interface{}) {
	l.add(LevelWarn, fmt.Sprintf(format, args...))
}

func (l *FakeLogger) Error(args ...interface{}) {
	l.add(LevelError, fmt.Sprint(args...))
}

func (l *FakeLogger) Errorf(format string, args ...interface{}) {
	l.add(LevelError, fmt.Sprintf(format, args...))
}

func (l *FakeLogger) add(level, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, LogEntry{Level: level, Message: message})
}

// Entries returns an independent snapshot of captured entries.
func (l *FakeLogger) Entries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]LogEntry(nil), l.entries...)
}

// Notification is one captured notification call.
type Notification struct {
	Title   string
	Content string
}

// FakeNotifier records notifications in order.
type FakeNotifier struct {
	mu            sync.Mutex
	notifications []Notification
}

func (n *FakeNotifier) Notify(title, content string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notifications = append(n.notifications, Notification{Title: title, Content: content})
}

// Notifications returns an independent snapshot of captured notifications.
func (n *FakeNotifier) Notifications() []Notification {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Notification(nil), n.notifications...)
}

type clockWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

// FakeClock is a deterministic manual clock. Its zero value starts at the Unix epoch.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []clockWaiter
}

// NewFakeClock creates a manual clock at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentTimeLocked()
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.currentTimeLocked()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- now
		return ch
	}
	c.waiters = append(c.waiters, clockWaiter{deadline: now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward and releases all waiters whose deadlines are due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.currentTimeLocked().Add(d)
	remaining := c.waiters[:0]
	for _, waiter := range c.waiters {
		if waiter.deadline.After(c.now) {
			remaining = append(remaining, waiter)
			continue
		}
		waiter.ch <- waiter.deadline
	}
	c.waiters = remaining
}

func (c *FakeClock) currentTimeLocked() time.Time {
	if c.now.IsZero() {
		return time.Unix(0, 0)
	}
	return c.now
}

// InstanceSinkCall is one captured Push or PushAll invocation.
type InstanceSinkCall struct {
	TriggerTime int64
	Instances   []*instance.Instance
}

// FakeInstanceSink is a configurable in-memory InstanceSink.
type FakeInstanceSink struct {
	mu sync.Mutex

	PushErr    error
	PushAllErr error
	GetAllErr  error
	RemoteList *instance.InstanceList

	pushCalls    []InstanceSinkCall
	pushAllCalls []InstanceSinkCall
}

func (s *FakeInstanceSink) Push(triggerTime int64, instances []*instance.Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushCalls = append(s.pushCalls, InstanceSinkCall{
		TriggerTime: triggerTime,
		Instances:   cloneInstances(instances),
	})
	return s.PushErr
}

func (s *FakeInstanceSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushAllCalls = append(s.pushAllCalls, InstanceSinkCall{
		TriggerTime: triggerTime,
		Instances:   cloneInstances(instances),
	})
	return s.PushAllErr
}

func (s *FakeInstanceSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneInstanceList(s.RemoteList), s.GetAllErr
}

// SetErrors configures the errors returned by Push, PushAll, and GetAll.
func (s *FakeInstanceSink) SetErrors(pushErr, pushAllErr, getAllErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PushErr = pushErr
	s.PushAllErr = pushAllErr
	s.GetAllErr = getAllErr
}

// SetRemoteList stores an independent copy for later GetAll calls.
func (s *FakeInstanceSink) SetRemoteList(list *instance.InstanceList) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RemoteList = cloneInstanceList(list)
}

// PushCalls returns independent copies of captured Push calls.
func (s *FakeInstanceSink) PushCalls() []InstanceSinkCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSinkCalls(s.pushCalls)
}

// PushAllCalls returns independent copies of captured PushAll calls.
func (s *FakeInstanceSink) PushAllCalls() []InstanceSinkCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSinkCalls(s.pushAllCalls)
}

// FakeInstanceSource is a configurable in-memory InstanceSource.
type FakeInstanceSource struct {
	mu sync.Mutex

	NameValue string
	All       []*instance.Instance
	WatchC    chan []*instance.Instance
	RunFn     func(context.Context) error

	pending  [][]*instance.Instance
	notify   chan struct{}
	stop     chan struct{}
	closed   bool
	pumpOnce sync.Once
	stopOnce sync.Once
}

// NewFakeInstanceSource creates a source with a buffered watch channel.
func NewFakeInstanceSource(name string, watchBuffer int) *FakeInstanceSource {
	if watchBuffer < 0 {
		watchBuffer = 0
	}
	s := &FakeInstanceSource{
		NameValue: name,
		WatchC:    make(chan []*instance.Instance, watchBuffer),
		notify:    make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	s.startPump()
	return s
}

func (s *FakeInstanceSource) Name() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.NameValue
}

func (s *FakeInstanceSource) Run(ctx context.Context) error {
	s.mu.Lock()
	fn := s.RunFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (s *FakeInstanceSource) Watch(ctx context.Context) <-chan []*instance.Instance {
	s.ensureInitialized()
	return s.WatchC
}

func (s *FakeInstanceSource) GetAll() []*instance.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneInstances(s.All)
}

// SetAll stores an independent copy returned by GetAll.
func (s *FakeInstanceSource) SetAll(instances []*instance.Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.All = cloneInstances(instances)
}

// Emit queues one watch update without blocking the caller.
func (s *FakeInstanceSource) Emit(instances ...*instance.Instance) {
	s.ensureInitialized()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.pending = append(s.pending, cloneInstances(instances))
	notify := s.notify
	s.mu.Unlock()

	select {
	case notify <- struct{}{}:
	default:
	}
}

// Close stops the source and closes its watch channel. It is idempotent.
func (s *FakeInstanceSource) Close() {
	s.ensureInitialized()
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.stop)
	})
}

func (s *FakeInstanceSource) ensureInitialized() {
	s.mu.Lock()
	if s.WatchC == nil {
		s.WatchC = make(chan []*instance.Instance, 16)
	}
	if s.notify == nil {
		s.notify = make(chan struct{}, 1)
	}
	if s.stop == nil {
		s.stop = make(chan struct{})
	}
	s.mu.Unlock()
	s.startPump()
}

func (s *FakeInstanceSource) startPump() {
	s.pumpOnce.Do(func() {
		go func() {
			defer close(s.WatchC)
			for {
				s.mu.Lock()
				if len(s.pending) == 0 {
					s.mu.Unlock()
					select {
					case <-s.notify:
						continue
					case <-s.stop:
						return
					}
				}
				next := s.pending[0]
				s.pending = s.pending[1:]
				s.mu.Unlock()

				select {
				case s.WatchC <- cloneInstances(next):
				case <-s.stop:
					return
				}
			}
		}()
	})
}

// FakeLeaderElector provides scripted and manually emitted transitions.
type FakeLeaderElector struct {
	mu sync.Mutex

	Reports []bool

	changes  chan<- bool
	pending  []bool
	notify   chan struct{}
	stop     chan struct{}
	done     chan struct{}
	started  bool
	stopped  bool
	pumpOnce sync.Once
	stopOnce sync.Once
}

// NewFakeLeaderElector creates an elector with scripted initial reports.
func NewFakeLeaderElector(reports ...bool) *FakeLeaderElector {
	return &FakeLeaderElector{
		Reports: append([]bool(nil), reports...),
		notify:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// ElectWait starts forwarding transitions and returns immediately.
func (e *FakeLeaderElector) ElectWait(changes chan<- bool) {
	e.ensureInitialized()
	e.mu.Lock()
	if !e.started && !e.stopped {
		e.started = true
		e.changes = changes
		e.pending = append(e.pending, e.Reports...)
	}
	notify := e.notify
	e.mu.Unlock()

	e.startPump()
	select {
	case notify <- struct{}{}:
	default:
	}
}

// Emit queues a leadership transition without blocking the caller.
func (e *FakeLeaderElector) Emit(isLeader bool) {
	e.ensureInitialized()
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	e.pending = append(e.pending, isLeader)
	notify := e.notify
	e.mu.Unlock()

	select {
	case notify <- struct{}{}:
	default:
	}
}

// Stop stops transition forwarding and waits for the pump to exit. It is idempotent.
func (e *FakeLeaderElector) Stop() {
	e.ensureInitialized()
	e.startPump()
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.stopped = true
		e.mu.Unlock()
		close(e.stop)
	})
	<-e.done
}

func (e *FakeLeaderElector) ensureInitialized() {
	e.mu.Lock()
	if e.notify == nil {
		e.notify = make(chan struct{}, 1)
	}
	if e.stop == nil {
		e.stop = make(chan struct{})
	}
	if e.done == nil {
		e.done = make(chan struct{})
	}
	e.mu.Unlock()
}

func (e *FakeLeaderElector) startPump() {
	e.pumpOnce.Do(func() {
		go func() {
			defer close(e.done)
			for {
				e.mu.Lock()
				if len(e.pending) == 0 || e.changes == nil {
					e.mu.Unlock()
					select {
					case <-e.notify:
						continue
					case <-e.stop:
						return
					}
				}
				next := e.pending[0]
				e.pending = e.pending[1:]
				changes := e.changes
				e.mu.Unlock()

				select {
				case changes <- next:
				case <-e.stop:
					return
				}
			}
		}()
	})
}

// FakeMetricsRecorder captures application metrics in memory.
type FakeMetricsRecorder struct {
	mu sync.Mutex

	syncOnceDurations []time.Duration
	syncAllDurations  map[string][]time.Duration
	queueDepths       []int
	syncOnceCount     int
}

// NewFakeMetricsRecorder creates an initialized metrics recorder.
func NewFakeMetricsRecorder() *FakeMetricsRecorder {
	return &FakeMetricsRecorder{syncAllDurations: make(map[string][]time.Duration)}
}

func (r *FakeMetricsRecorder) ObserveSyncOnceDuration(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncOnceDurations = append(r.syncOnceDurations, d)
}

func (r *FakeMetricsRecorder) ObserveSyncAllDuration(provider string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.syncAllDurations == nil {
		r.syncAllDurations = make(map[string][]time.Duration)
	}
	r.syncAllDurations[provider] = append(r.syncAllDurations[provider], d)
}

func (r *FakeMetricsRecorder) SetSyncErrorQueueDepth(depth int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queueDepths = append(r.queueDepths, depth)
}

func (r *FakeMetricsRecorder) MarkSyncOnce() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncOnceCount++
}

// SyncOnceDurations returns an independent snapshot of recorded durations.
func (r *FakeMetricsRecorder) SyncOnceDurations() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.syncOnceDurations...)
}

// SyncAllDurations returns an independent snapshot for one provider.
func (r *FakeMetricsRecorder) SyncAllDurations(provider string) []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.syncAllDurations[provider]...)
}

// QueueDepths returns every recorded queue depth.
func (r *FakeMetricsRecorder) QueueDepths() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.queueDepths...)
}

// SyncOnceCount returns the number of MarkSyncOnce calls.
func (r *FakeMetricsRecorder) SyncOnceCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.syncOnceCount
}

// FakeEventQueue is an in-memory highest-Reversion-wins retry queue.
type FakeEventQueue struct {
	mu     sync.Mutex
	events map[string]*ports.Event
}

// NewFakeEventQueue creates an initialized queue.
func NewFakeEventQueue() *FakeEventQueue {
	return &FakeEventQueue{events: make(map[string]*ports.Event)}
}

func (q *FakeEventQueue) Add(triggerTime int64, instances []*instance.Instance) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.events == nil {
		q.events = make(map[string]*ports.Event)
	}
	for _, item := range instances {
		if item == nil {
			continue
		}
		old, exists := q.events[item.InstanceId]
		if exists && len(old.Data) > 0 && old.Data[0] != nil && item.Reversion <= old.Data[0].Reversion {
			continue
		}
		q.events[item.InstanceId] = &ports.Event{
			Trigger: triggerTime,
			Data:    cloneInstances([]*instance.Instance{item}),
			Operate: ports.OperateTypeSync,
		}
	}
}

func (q *FakeEventQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.events)
}

// Drain returns an independent snapshot without removing queued events.
func (q *FakeEventQueue) Drain() []*ports.Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*ports.Event, 0, len(q.events))
	for _, event := range q.events {
		result = append(result, cloneEvent(event))
	}
	return result
}

// Remove deletes one instance ID from the queue.
func (q *FakeEventQueue) Remove(instanceID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.events, instanceID)
}

func cloneSinkCalls(calls []InstanceSinkCall) []InstanceSinkCall {
	result := make([]InstanceSinkCall, len(calls))
	for i, call := range calls {
		result[i] = InstanceSinkCall{
			TriggerTime: call.TriggerTime,
			Instances:   cloneInstances(call.Instances),
		}
	}
	return result
}

func cloneEvent(event *ports.Event) *ports.Event {
	if event == nil {
		return nil
	}
	return &ports.Event{
		Trigger: event.Trigger,
		Data:    cloneInstances(event.Data),
		Operate: event.Operate,
	}
}

func cloneInstanceList(list *instance.InstanceList) *instance.InstanceList {
	if list == nil {
		return nil
	}
	return &instance.InstanceList{Instance: cloneInstances(list.Instance)}
}

func cloneInstances(instances []*instance.Instance) []*instance.Instance {
	if instances == nil {
		return nil
	}
	result := make([]*instance.Instance, len(instances))
	for i, item := range instances {
		result[i] = cloneInstance(item)
	}
	return result
}

func cloneInstance(item *instance.Instance) *instance.Instance {
	if item == nil {
		return nil
	}
	result := *item
	if item.Ports != nil {
		result.Ports = make([]*instance.PortInfo, len(item.Ports))
		for i, port := range item.Ports {
			if port != nil {
				copied := *port
				result.Ports[i] = &copied
			}
		}
	}
	result.Label = cloneStringMap(item.Label)
	result.Image = cloneStringMap(item.Image)
	return &result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

var _ ports.Logger = (*FakeLogger)(nil)
var _ ports.Notifier = (*FakeNotifier)(nil)
var _ ports.Clock = (*FakeClock)(nil)
var _ ports.InstanceSink = (*FakeInstanceSink)(nil)
var _ ports.InstanceSource = (*FakeInstanceSource)(nil)
var _ ports.LeaderElector = (*FakeLeaderElector)(nil)
var _ ports.MetricsRecorder = (*FakeMetricsRecorder)(nil)
var _ ports.EventQueue = (*FakeEventQueue)(nil)
