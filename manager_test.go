// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestManagerDefaults(t *testing.T) {
	m := New()
	if m.st == nil {
		t.Fatalf("want Store, got %v", m.st)
	}
	if want, got := defaultConcurrency, m.concurrency; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := defaultPollInterval, m.interval; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestManagerStartAndStop(t *testing.T) {
	var start, stop time.Time
	testPollerStarted = func() {
		start = time.Now()
	}
	testPollerStopped = func() {
		stop = time.Now()
	}
	st := NewInMemoryStore()
	m := New(SetStore(st))
	err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	err = m.Close()
	if err != nil {
		t.Fatal(err)
	}
	if start.IsZero() {
		t.Error("expected poller to be started")
	}
	if stop.IsZero() {
		t.Error("expected poller to be stopped")
	}
	if !stop.After(start) {
		t.Error("expected poller to be stopped after being started")
	}
}

func TestManagerRegisterDuplicate(t *testing.T) {
	st := NewInMemoryStore()
	m := New(SetStore(st))
	err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	err = m.Register("topic", func(args ...interface{}) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Register("topic", func(args ...interface{}) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expected error, got %v", err)
	}
}

func TestManagerEnqueue(t *testing.T) {
	st := NewInMemoryStore()
	m := New(SetStore(st))
	err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	err = m.Register("topic", func(args ...interface{}) error {
		if len(args) != 1 {
			return fmt.Errorf("len(args) == %d", len(args))
		}
		if s := args[0].(string); s != "Hello" {
			return fmt.Errorf("args == %v", args)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	task := &Task{Topic: "topic", Args: []interface{}{"Hello"}}
	err = m.Enqueue(task)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if got := st.input[0].ID; got == "" {
		t.Errorf("expected to generate an ID, got %q", got)
	}
}

func TestManagerProcessorSuccess(t *testing.T) {
	interval := 1 * time.Second
	st := NewInMemoryStore()
	m := New(SetStore(st), SetPollInterval(interval), SetConcurrency(3))
	var processed int
	err := m.Register("topic", func(args ...interface{}) error {
		time.Sleep(interval)
		processed++
		if len(args) != 1 {
			return fmt.Errorf("len(args) == %d", len(args))
		}
		if s := args[0].(string); s != "Hello" {
			return fmt.Errorf("args == %v", args)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Start()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	task := &Task{
		Topic: "topic",
		Args:  []interface{}{"Hello"},
	}
	err = m.Enqueue(task)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, len(st.input); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if got := st.input[0].ID; got == "" {
		t.Errorf("expected to generate an ID, got %q", got)
	}
	time.Sleep(interval)
	time.Sleep(100 * time.Millisecond)
	// Task should be picked up by poller and put into work queue
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	size, err := st.SizeOfWorkQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, size; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	time.Sleep(interval)
	time.Sleep(100 * time.Millisecond)
	if want, got := 1, processed; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestManagerProcessorFailure(t *testing.T) {
	interval := 1 * time.Second
	st := NewInMemoryStore()
	m := New(SetStore(st), SetPollInterval(interval), SetConcurrency(3))
	err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	var processed int
	err = m.Register("topic", func(args ...interface{}) error {
		processed++
		return errors.New("kaboom")
	})
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	const R = 5
	task := &Task{
		Topic:      "topic",
		Args:       []interface{}{"Hello"},
		NumRetries: R,
	}
	err = m.Enqueue(task)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, len(st.input); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	spec := st.input[0]
	if got := spec.ID; got == "" {
		t.Errorf("expected to generate an ID, got %q", got)
	}
	time.Sleep(interval)
	time.Sleep(100 * time.Millisecond)
	// Task should be picker up by poller and put into work queue,
	// then back into input queue with a retry of R-1
	if want, got := 1, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	size, err := st.SizeOfWorkQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, size; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, processed; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	next := st.input[0]
	if next == nil {
		t.Fatalf("expected spec, got %v", next)
	}
	if want, got := spec.ID, next.ID; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := 1, next.Retry; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := R, next.NumRetries; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestManagerProcessorDeadQueue(t *testing.T) {
	interval := 1 * time.Second
	st := NewInMemoryStore()
	m := New(SetStore(st), SetPollInterval(interval), SetConcurrency(3))
	err := m.Start()
	if err != nil {
		t.Fatal(err)
	}
	var processed int
	err = m.Register("topic", func(args ...interface{}) error {
		processed++
		return errors.New("kaboom")
	})
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	task := &Task{
		Topic:      "topic",
		Args:       []interface{}{1, 2},
		NumRetries: 0,
	}
	err = m.Enqueue(task)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, len(st.input); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	spec := st.input[0]
	if got := spec.ID; got == "" {
		t.Errorf("expected to generate an ID, got %q", got)
	}
	time.Sleep(interval)
	time.Sleep(100 * time.Millisecond)
	// Task should be picker up by poller and put into work queue,
	// then into dead queue as retry is 0
	if want, got := 1, len(st.dead); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	size, err := st.SizeOfWorkQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, size; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, processed; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	next := st.dead[0]
	if next == nil {
		t.Fatalf("expected spec, got %v", next)
	}
	if want, got := spec.ID, next.ID; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := 0, next.Retry; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := 0, next.NumRetries; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}
