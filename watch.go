// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import (
	"sync"
	"time"
)

const (
	// ManagerStart event type is triggered on manager startup.
	ManagerStart = "MANAGER_START"
	// ManagerStop event type is triggered on manager shutdown.
	ManagerStop = "MANAGER_STOP"
	// ManagerStats event type returns global stats periodically.
	ManagerStats = "MANAGER_STATS"
	// TaskEnqueue event type is triggered when a new task is enqueued.
	TaskEnqueue = "TASK_ENQUEUE"
	// TaskStart event type is triggered when a new task is started.
	TaskStart = "TASK_START"
	// TaskRetry event type is triggered when a task is retried.
	TaskRetry = "TASK_RETRY"
	// TaskCompletion event type is triggered when a task completed successfully.
	TaskCompletion = "TASK_COMPLETION"
	// TaskFailure event type is triggered when a task has failed.
	TaskFailure = "TASK_FAILURE"
)

// WatchEvent is send to consumers watching the manager after
// calling Watch on the manager.
type WatchEvent struct {
	Type  string    `json:"type"`            // event type
	Task  *TaskSpec `json:"task,omitempty"`  // task details
	Stats *Stats    `json:"stats,omitempty"` // statistics
}

// Watch enables consumers to watch events happening inside a manager.
// Watch returns a channel of WatchEvents that it will send on.
// The caller must pass a done channel that it needs to close if it is
// no longer interested in watching events.
func (m *Manager) Watch(done chan struct{}) <-chan *WatchEvent {
	// We initialize two channels here: One for the events from the
	// taskqueue (events), and one for the ManagerStats events (statsev).
	// Finally, we merge both channels together so that they appear as
	// one simple channel.

	events := make(chan *WatchEvent)
	go m.st.Subscribe(events)

	statsev := make(chan *WatchEvent)

	go func() {
		// TODO Make this configurable
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-done:
				// Stop watching
				close(statsev)
				close(events)
				return
			case <-t.C:
				st, err := m.Stats()
				if err != nil {
					// No stats
					break
				}
				statsev <- &WatchEvent{Type: ManagerStats, Stats: st}
			}
		}
	}()

	// merge both channels, and stop if done receives a value
	return mergeWatchEvents(done, events, statsev)
}

// mergeWatchEvents merges one or more input channels of WatchEvents together
// and returns them as a single channel.
// See https://blog.golang.org/pipelines for details on the implementation.
func mergeWatchEvents(done <-chan struct{}, cs ...<-chan *WatchEvent) <-chan *WatchEvent {
	var wg sync.WaitGroup
	out := make(chan *WatchEvent)

	// Start an output goroutine for each input channel in cs.
	// output copies values from c to out until c is closed or it
	// receives a value from done, then output calls wg.Done.
	output := func(c <-chan *WatchEvent) {
		for n := range c {
			select {
			case out <- n:
			case <-done:
			}
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are done.
	// This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
