// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

// Task specifies a task to execute.
type Task struct {
	// Topic for this task.
	Topic string
	// ExternalID is an application-specific identifier for a task.
	ExternalID string
	// Args is the list of arguments passed to the Processor.
	Args []interface{}
	// NumRetries specifies the number of retries in case of failures
	// in the processor.
	NumRetries int
}

// TaskSpec is the internal representation of a Task state.
type TaskSpec struct {
	ID         string        `json:"id"`
	Topic      string        `json:"topic"`
	ExternalID string        `json:"xid,omitempty"`
	Args       []interface{} `json:"args"`
	Retry      int           `json:"retry"`       // current retry
	NumRetries int           `json:"num_retries"` // max. number of retries
	Enqueued   int64         `json:"enqueued"`    // time the task has been enqueued
	Priority   int64         `json:"priority"`    // lower means: execute earlier
}
