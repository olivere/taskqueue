# Taskqueue

**This is just an experiment and not ready for production.**

Taskqueue manages running and scheduling tasks (think Sidekiq or Resque).

[![Build Status](https://travis-ci.org/olivere/taskqueue.svg?branch=master)](https://travis-ci.org/olivere/taskqueue)
[![Godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](http://godoc.org/github.com/olivere/taskqueue)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/olivere/taskqueue/master/LICENSE)

## Prerequisites

Redis is currently only persistent storage backend. So you must have Redis
installed.

## Getting started

Get the repository with `go get github.com/olivere/taskqueue`.

Example:

```go
// Create a Redis-based backend, namespaced with "taskqueue_example"
store := taskqueue.NewRedisStore("localhost:6379", "taskqueue_example", "", 0)

// Create a manager with the Redis store. See source for more options.
m := taskqueue.New(taskqueue.SetStore(store))

// Register one or more topics and their processor
m.Register("clicks", func(args ...interface{}) error {
	// Handle "clicks" topic
})

// Start the manager
err := m.Start()
if err != nil {
	panic(err)
}

// Enqueue a task: It'll be added to the input queue and processed eventually
err = m.Enqueue(&taskqueue.Task{Topic: "clicks", Args: []interface{}{640, 480}})
if err != nil {
	panic(err)
}

...

// Stop the manager, either via Close (which stops immediately)
// or CloseWithTimeout (which gracefully waits until either all working
// tasks are completed or a timeout is reached)
err = m.CloseWithTimeout(15 * time.Second)
if err != nil {
	panic(err)
}
```

See the tests for more details on using taskqueue.

## Tests and Web UI

Ensure the tests succeed with `go test`. You may have to install dependencies.

Run an end-to-end test with `go run e2e/main.go`. It simulates a real worker.
Play with the options: `go run e2e/main.go -h`.

While running the end-to-end tests, open up a second console and run
`go run ui/main.go`. Then direct your web browser to `127.0.0.1:12345`.

![Screenshot](https://raw.githubusercontent.com/olivere/taskqueue/master/images/screenshot1.png)

# License

MIT License. See LICENSE file for details.
