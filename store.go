// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

type Store interface {
	// Enqueue adds the task specification to the input queue.
	Enqueue(*TaskSpec) error

	// Dequeue removes the task specification from the work queue.
	Dequeue(id string) error

	// Next checks for tasks in the input queue and moves it into the
	// work queue. If there are no tasks in the input queue, nil is returned.
	Next() (*TaskSpec, error)

	// Retry takes a task specification and moves it from the work queue
	// back into the input queue.
	Retry(*TaskSpec) error

	// MoveToDeadQueue moves the task from the work queue to the dead queue.
	MoveToDeadQueue(*TaskSpec) error

	// Move all items from work queue to input queue (used at startup).
	MoveWorkQueueToInputQueue() error

	// SizeOfInputQueue returns the current number of items in the input queue.
	SizeOfInputQueue() (int, error)

	// SizeOfWorkQueue returns the current number of items in the work queue.
	SizeOfWorkQueue() (int, error)

	// SizeOfDeadQueue returns the current number of items in the dead queue.
	SizeOfDeadQueue() (int, error)

	// Stats returns a snapshot of the currently stored statistics.
	StatsSnapshot() (*Stats, error)

	// StatsIncrement increments a given statistic.
	StatsIncrement(field StatsField, delta int) error

	// Publish publishes an event.
	Publish(payload *WatchEvent) error

	// Subscribe subscribes to the list of changes.
	Subscribe(recv chan *WatchEvent)
}

// StatsField represents a metrics.
type StatsField string

const (
	EnqueuedField  StatsField = "enqueued"
	StartedField   StatsField = "started"
	RetriedField   StatsField = "retried"
	FailedField    StatsField = "failed"
	CompletedField StatsField = "completed"
)

// Stats represents statistics.
type Stats struct {
	Enqueued       int `json:"enqueued"`  // put into input queue
	Started        int `json:"started"`   // started processor
	Retried        int `json:"retried"`   // failed but still retrying
	Failed         int `json:"failed"`    // finally failed and moved to dead queue
	Completed      int `json:"completed"` // completed successfully
	InputQueueSize int `json:"input_queue_size"`
	WorkQueueSize  int `json:"work_queue_size"`
	DeadQueueSize  int `json:"dead_queue_size"`
}
