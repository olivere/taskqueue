// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import "time"

const (
	// ManagerStart event type is triggered on manager startup.
	ManagerStart = "MANAGER_START"
	// ManagerStop event type is triggered on manager shutdown.
	ManagerStop = "MANAGER_STOP"
	// ManagerStats event type returns global stats periodically.
	ManagerStats = "MANAGER_STATS"
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
	events := make(chan *WatchEvent)
	go m.st.Subscribe(events)

	go func() {
		// TODO Make this configurable
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-done:
				// Stop watching
				close(events)
				return
			case <-t.C:
				st, err := m.Stats()
				if err != nil {
					// No stats
					break
				}
				events <- &WatchEvent{Type: ManagerStats, Stats: st}
			}
		}
	}()
	return events
}
