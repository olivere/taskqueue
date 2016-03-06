// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import "sync"

// InMemoryStore is a simple in-memory storage backend.
// It is used in tests only.
type InMemoryStore struct {
	mu      sync.Mutex
	input   []*TaskSpec
	work    []*TaskSpec
	dead    []*TaskSpec
	metrics map[StatsField]int
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		input:   make([]*TaskSpec, 0),
		work:    make([]*TaskSpec, 0),
		dead:    make([]*TaskSpec, 0),
		metrics: make(map[StatsField]int),
	}
}

func (r *InMemoryStore) Enqueue(spec *TaskSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.input = append(r.input, spec)
	return nil
}

func (r *InMemoryStore) Dequeue(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, spec := range r.work {
		if spec.ID == id {
			r.work = append(r.work[:i], r.work[i+1:]...)
			break
		}
	}
	return nil
}

func (r *InMemoryStore) Next() (*TaskSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.input) == 0 {
		return nil, nil
	}
	spec := r.input[0]
	r.work = append(r.work, spec)
	r.input = r.input[1:]
	return spec, nil
}

func (r *InMemoryStore) Retry(spec *TaskSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, ts := range r.work {
		if ts.ID == spec.ID {
			r.work = append(r.work[:i], r.work[i+1:]...)
			r.input = append(r.input, spec)
			return nil
		}
	}
	return nil
}

func (r *InMemoryStore) MoveToDeadQueue(spec *TaskSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, ts := range r.work {
		if ts.ID == spec.ID {
			r.work = append(r.work[:i], r.work[i+1:]...)
			r.dead = append(r.dead, spec)
			return nil
		}
	}
	return nil
}

// Move all items from work queue to input queue (used at startup).
func (r *InMemoryStore) MoveWorkQueueToInputQueue() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, spec := range r.work {
		r.input = append(r.input, spec)
	}
	r.work = make([]*TaskSpec, 0)
	return nil
}

func (r *InMemoryStore) SizeOfInputQueue() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.input), nil
}

func (r *InMemoryStore) SizeOfWorkQueue() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.work), nil
}

func (r *InMemoryStore) SizeOfDeadQueue() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.dead), nil
}

func (r *InMemoryStore) StatsSnapshot() (*Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := new(Stats)
	for key, value := range r.metrics {
		switch key {
		case EnqueuedField:
			st.Enqueued = value
		case StartedField:
			st.Started = value
		case RetriedField:
			st.Retried = value
		case FailedField:
			st.Failed = value
		case CompletedField:
			st.Completed = value
		}
	}
	st.InputQueueSize = len(r.input)
	st.WorkQueueSize = len(r.work)
	st.DeadQueueSize = len(r.dead)
	return st, nil
}

func (r *InMemoryStore) StatsIncrement(f StatsField, delta int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, found := r.metrics[f]; !found {
		r.metrics[f] = delta
	} else {
		r.metrics[f] = v + delta
	}
	return nil
}

func (r *InMemoryStore) Publish(e *WatchEvent) error {
	return nil
}

func (r *InMemoryStore) Subscribe(recv chan *WatchEvent) {
}
