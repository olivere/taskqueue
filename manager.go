// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

const (
	defaultPollInterval = 2 * time.Second
	defaultConcurrency  = 5
)

func nop() {}

var (
	testPollerStarted = nop // testing hook
	testPollerStopped = nop // testing hook
)

// Manager manages tasks.
type Manager struct {
	mu          sync.Mutex
	started     bool
	logger      Logger
	tm          map[string]Processor // key: topic
	st          Store
	interval    time.Duration
	concurrency int
	backoff     BackoffFunc
	pollerc     chan struct{}
	workers     []*worker
	workersWg   sync.WaitGroup
	workc       chan *TaskSpec
}

// New creates a new manager.
//
// Configure the manager with Set methods.
// Example:
//     m := taskqueue.New(taskqueue.SetStore(...), taskqueue.SetPollInterval(...))
func New(options ...ManagerOption) *Manager {
	m := &Manager{
		tm:          make(map[string]Processor),
		logger:      stdLogger{},
		st:          NewInMemoryStore(),
		interval:    defaultPollInterval,
		concurrency: defaultConcurrency,
		backoff:     exponentialBackoff,
		started:     false,
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// Stats returns a snapshot of the current statistics, e.g. the number
// of started and completed tasks.
func (m *Manager) Stats() (*Stats, error) {
	return m.st.StatsSnapshot()
}

// ManagerOption is an options provider to be used when creating a
// new task manager.
type ManagerOption func(*Manager)

// SetStore specifies the data store to use for storing task information.
// The default is RedisStore.
func SetStore(store Store) ManagerOption {
	return func(m *Manager) {
		m.st = store
	}
}

// SetLogger specifies the logger to use when reporting.
func SetLogger(logger Logger) ManagerOption {
	return func(m *Manager) {
		m.logger = logger
	}
}

// SetPollInterval specifies the interval at which the manager polls for jobs.
func SetPollInterval(interval time.Duration) ManagerOption {
	return func(m *Manager) {
		m.interval = interval
	}
}

// SetConcurrency specifies the number of workers working in parallel.
// Concurrency must be greater or equal to 1 and is 25 by default.
func SetConcurrency(n int) ManagerOption {
	return func(m *Manager) {
		if n <= 1 {
			n = 1
		}
		m.concurrency = n
	}
}

// SetBackoffFunc specifies the backoff function that returns the timespan
// between retries of failed jobs. Exponential backoff is used by default.
func SetBackoffFunc(fn BackoffFunc) ManagerOption {
	return func(m *Manager) {
		if fn == nil {
			m.backoff = exponentialBackoff
		} else {
			m.backoff = fn
		}
	}
}

// Register registers a topic and the associated processor.
func (m *Manager) Register(topic string, p Processor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.tm[topic]; found {
		return fmt.Errorf("topic %q already exists", topic)
	}
	m.tm[topic] = p
	return nil
}

// Start runs the task manager. Use Close to stop it.
// It is the callers responsibility to ensure that only one worker
// per namespace is started at any point in time.
func (m *Manager) Start() error {
	// Move stalled jobs from previous run back into input queue
	err := m.st.MoveWorkQueueToInputQueue()
	if err != nil {
		return err
	}

	m.workc = make(chan *TaskSpec, m.concurrency)
	m.workers = make([]*worker, m.concurrency)
	for i := 0; i < m.concurrency; i++ {
		m.workersWg.Add(1)
		m.workers[i] = newWorker(m, m.workc)
	}
	m.pollerc = make(chan struct{})
	go m.poller()

	m.mu.Lock()
	m.started = true
	m.mu.Unlock()

	m.st.Publish(&WatchEvent{Type: ManagerStart})

	return nil
}

// Close stops the task manager immediately, canceling all active workers
// immediately. If you want graceful shutdown, use CloseWithTimeout.
func (m *Manager) Close() error {
	return m.CloseWithTimeout(-1 * time.Second)
}

// CloseWithTimeout is like Close but waits until all running tasks are completed.
// New tasks are no longer accepted. This ensures a graceful shutdown.
// Use a negative timeout to wait indefinitely.
func (m *Manager) CloseWithTimeout(timeout time.Duration) (err error) {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Stop accepting new tasks
	m.pollerc <- struct{}{}
	<-m.pollerc
	close(m.pollerc)
	close(m.workc)

	if timeout.Nanoseconds() < 0 {
		// Wait until all workers are completed
		m.workersWg.Wait()
		m.st.Publish(&WatchEvent{Type: ManagerStop})
		return
	}

	// Wait with timeout
	complete := make(chan struct{}, 1)
	go func() {
		// Stop workers
		m.workersWg.Wait()
		close(complete)
	}()
	select {
	case <-complete:
		// Completed in time
		err = nil
	case <-time.After(timeout):
		// Time out waiting for active tasks
		err = errors.New("timeout")
	}
	m.mu.Lock()
	m.started = false
	m.mu.Unlock()

	m.st.Publish(&WatchEvent{Type: ManagerStop})
	return
}

// specFromTask initializes a TaskSpec from a Task.
func specFromTask(task *Task) *TaskSpec {
	now := time.Now().UnixNano()
	return &TaskSpec{
		ID:         uuid.NewV4().String(),
		ExternalID: task.ExternalID,
		Topic:      task.Topic,
		Args:       task.Args,
		Retry:      0,
		NumRetries: task.NumRetries,
		Enqueued:   now,
		Priority:   now,
	}
}

// Enqueue adds a task to the input queue. It will be picked up and run
// eventually. Tasks are processed ordered by time.
func (m *Manager) Enqueue(task *Task) error {
	if task.Topic == "" {
		return errors.New("no topic specified")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, found := m.tm[task.Topic]
	if !found {
		return fmt.Errorf("no such topic: %q", task.Topic)
	}
	err := m.st.Enqueue(specFromTask(task))
	if err != nil {
		return err
	}
	m.st.StatsIncrement(EnqueuedField, 1)
	return nil
}

// poller periodically watches the input queue, picks up tasks, and passes
// them to idle workers.
func (m *Manager) poller() {
	testPollerStarted()
	defer testPollerStopped()

	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for {
				// Always try to fill up all available workers
				n, err := m.st.SizeOfWorkQueue()
				if err != nil {
					m.logger.Printf("Error determining size of work queue: %v", err)
					break
				}
				if n >= m.concurrency {
					// All workers busy
					break
				}
				spec, err := m.st.Next()
				if err != nil {
					m.logger.Printf("Error picking next task: %v", err)
					break
				}
				if spec == nil {
					// No task to execute
					break
				}
				m.workc <- spec
			}
		case <-m.pollerc:
			m.pollerc <- struct{}{}
			return
		}
	}
}

// -- worker --

// worker processes one Task via the Processor of its topic.
type worker struct {
	m     *Manager
	workc <-chan *TaskSpec // manager passes works here
	wg    sync.WaitGroup   // manager waits on wg at shutdown
}

func newWorker(m *Manager, workc <-chan *TaskSpec) *worker {
	w := &worker{
		m:     m,
		workc: workc,
	}
	go w.run()
	return w
}

func (w *worker) run() {
	defer w.m.workersWg.Done()
	for {
		select {
		case spec, more := <-w.workc:
			if !more {
				// Closed work channel
				return
			}
			err := w.process(spec)
			if err != nil {
				// This is a hard error: Retry and dead queue is covered by process
				w.m.logger.Printf("Error processing task %v: %v", spec.ID, err)
			}
		}
	}
}

func (w *worker) process(spec *TaskSpec) error {
	w.m.mu.Lock()
	p, found := w.m.tm[spec.Topic]
	w.m.mu.Unlock()
	if !found {
		// No processor for topic
		return fmt.Errorf("no processor for topic %q", spec.Topic)
	}

	w.m.st.StatsIncrement(StartedField, 1)
	w.m.st.Publish(&WatchEvent{Type: TaskStart, Task: spec})

	err := p(spec.Args...)
	if err != nil {
		// Retry or Dead queue?
		if spec.Retry < spec.NumRetries {
			// Retry
			w.m.st.StatsIncrement(RetriedField, 1)
			w.m.st.Publish(&WatchEvent{Type: TaskRetry, Task: spec})

			// Change Priority so that the task will be enqueued last
			spec.Priority = time.Now().Add(w.m.backoff(spec.Retry)).UnixNano()
			spec.Retry++
			err = w.m.st.Retry(spec)
			if err != nil {
				return err
			}
			return nil
		} else {
			w.m.st.StatsIncrement(FailedField, 1)
			w.m.st.Publish(&WatchEvent{Type: TaskFailure, Task: spec})

			// Dead queue
			err = w.m.st.MoveToDeadQueue(spec)
			if err != nil {
				return err
			}
			return nil
		}
	}

	w.m.st.StatsIncrement(CompletedField, 1)
	w.m.st.Publish(&WatchEvent{Type: TaskCompletion, Task: spec})

	err = w.m.st.Dequeue(spec.ID)
	if err != nil {
		// TODO(oe) How do we report errors
		return err
	}
	return nil
}
