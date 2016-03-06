// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

// //+build integration
package taskqueue

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
	"time"

	dispredis "github.com/EverythingMe/disposable-redis"
	"github.com/garyburd/redigo/redis"
)

const testNamespace = "taskqueue_test"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func fakeRedis() (*dispredis.Server, error) {
	// Silence log output
	log.SetOutput(ioutil.Discard)
	r, err := dispredis.NewServerRandomPort()
	if err != nil {
		return nil, err
	}
	if err = r.WaitReady(50 * time.Millisecond); err != nil {
		return nil, err
	}
	return r, nil
}

func getTask(t testing.TB, st *RedisStore, c redis.Conn, id string) *TaskSpec {
	data, err := redis.String(c.Do("GET", st.key("tasks", id)))
	if err != nil {
		t.Fatal(err)
	}
	var v TaskSpec
	err = json.Unmarshal([]byte(data), &v)
	if err != nil {
		t.Fatal(err)
	}
	return &v
}

func inputQKeys(t testing.TB, st *RedisStore, c redis.Conn) []string {
	v, err := redis.Strings(c.Do("ZRANGE", st.key("queue", "input"), "0", "-1"))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func inputQSize(t testing.TB, st *RedisStore, c redis.Conn) int {
	n, err := redis.Int(c.Do("ZCARD", st.key("queue", "input")))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func inputQExists(t testing.TB, st *RedisStore, c redis.Conn, id string) bool {
	_, err := redis.Int(c.Do("ZRANK", st.key("queue", "input"), id))
	return err != redis.ErrNil
}

func workQSize(t testing.TB, st *RedisStore, c redis.Conn) int {
	n, err := redis.Int(c.Do("SCARD", st.key("queue", "work")))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func workQKeys(t testing.TB, st *RedisStore, c redis.Conn) []string {
	v, err := redis.Strings(c.Do("SMEMBERS", st.key("queue", "work")))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func workQExists(t testing.TB, st *RedisStore, c redis.Conn, id string) bool {
	n, err := redis.Int(c.Do("SISMEMBER", st.key("queue", "work"), id))
	if err != nil {
		return false
	}
	return n == 1
}

func deadQSize(t testing.TB, st *RedisStore, c redis.Conn) int {
	n, err := redis.Int(c.Do("SCARD", st.key("queue", "dead")))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func deadQKeys(t testing.TB, st *RedisStore, c redis.Conn) []string {
	v, err := redis.Strings(c.Do("SMEMBERS", st.key("queue", "dead")))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func deadQExists(t testing.TB, st *RedisStore, c redis.Conn, id string) bool {
	n, err := redis.Int(c.Do("SISMEMBER", st.key("queue", "dead"), id))
	if err != nil {
		return false
	}
	return n == 1
}

func TestRedisKeys(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	var tests = []struct {
		NS   string
		In   []string
		Want string
	}{
		{"", nil, ""},
		{"", []string{""}, ""},
		{"taskqueue_test", []string{"tasks", "123"}, "taskqueue_test:tasks:123"},
	}
	for _, test := range tests {
		st := NewRedisStore(r.Addr(), test.NS, "", 0)
		got := st.key(test.In...)
		if got != test.Want {
			t.Errorf("want %q, got %q", test.Want, got)
		}
	}
}

func TestRedisStoreNew(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	if want, got := testNamespace, st.ns; want != got {
		t.Errorf("want %q, got %q", want, got)
	}

	c := st.pool.Get()
	defer c.Close()
	keys := inputQKeys(t, st, c)
	if want, got := 0, len(keys); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestRedisStoreEnqueue(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	spec := specFromTask(&Task{Topic: "topic"})
	err = st.Enqueue(spec)
	if err != nil {
		t.Fatal(err)
	}

	c := st.pool.Get()
	defer c.Close()
	out := getTask(t, st, c, spec.ID)
	if want, got := spec.ID, out.ID; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
	if want, got := 1, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, workQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestRedisStoreDequeue(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	// Enqueue some tasks
	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	const N = 5
	var specs []*TaskSpec
	for i := 0; i < N; i++ {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		spec := specFromTask(&Task{Topic: "topic"})
		err = st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
	}

	// Pick one to fill the work queue
	_, err = st.Next()
	if err != nil {
		t.Fatal(err)
	}

	c := st.pool.Get()
	defer c.Close()
	if want, got := N-1, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	keys := workQKeys(t, st, c)
	if want, got := 1, len(keys); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	err = st.Dequeue(keys[0])
	if err != nil {
		t.Fatal(err)
	}

	// Work queue should also be empty
	if want, got := 0, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
}

func TestRedisStoreNextEmpty(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()

	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	spec, err := st.Next()
	if err != nil {
		t.Fatal(err)
	}
	if spec != nil {
		t.Fatalf("want %v, got %v", nil, err)
	}
}

func TestRedisStoreNextOneAvailable(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()

	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}

	spec := specFromTask(&Task{Topic: "topic"})
	err = st.Enqueue(spec)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}

	out, err := st.Next()
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatalf("want %v, got %v", spec, out)
	}
	if want, got := spec.ID, out.ID; want != got {
		t.Fatalf("want %v, got %v", want, got)
	}

	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, workQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestRedisStoreNextManyAvailable(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	// Enqueue some tasks
	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()
	const N = 5
	for i := 0; i < N; i++ {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		spec := specFromTask(&Task{Topic: "topic"})
		err = st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
	}

	keys := inputQKeys(t, st, c)
	if want, got := N, len(keys); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	for i := 0; i < N; i++ {
		out, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatalf("got %v", out)
		}
		if want, got := N-1-i, inputQSize(t, st, c); want != got {
			t.Errorf("want %d, got %d", want, got)
		}
		if want, got := i+1, workQSize(t, st, c); want != got {
			t.Errorf("want %d, got %d", want, got)
		}
	}
}

func TestRedisStoreRetry(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()

	if want, got := 0, workQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}

	// Enqueue
	var specs []*TaskSpec
	const N = 5
	for i := 0; i < N; i++ {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		spec := specFromTask(&Task{Topic: "topic"})
		err = st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
	}
	if want, got := N, len(specs); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	// Next to simulate work
	for i := 0; i < N; i++ {
		spec, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if spec == nil {
			t.Fatalf("expected spec, got %v", spec)
		}
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	// Retry the first
	err = st.Retry(specs[0])
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 4, workQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, inputQSize(t, st, c); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if ex := workQExists(t, st, c, specs[0].ID); ex {
		t.Errorf("expected id %q to not be in work queue", specs[0].ID)
	}
	if ex := inputQExists(t, st, c, specs[0].ID); !ex {
		t.Errorf("expected id %q in input queue", specs[0].ID)
	}
}

func TestRedisStoreMoveToDeadQueue(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()

	// Enqueue
	var specs []*TaskSpec
	const N = 5
	for i := 0; i < N; i++ {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		spec := specFromTask(&Task{Topic: "topic"})
		err = st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
	}
	if want, got := N, len(specs); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, deadQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	// Next to simulate work
	for i := 0; i < N; i++ {
		spec, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if spec == nil {
			t.Fatalf("expected spec, got %v", spec)
		}
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, deadQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	// Now move one from the worker queue to the dead queue
	err = st.MoveToDeadQueue(specs[0])
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N-1, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 1, deadQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
}

func TestRedisStoreMoveWorkQueueToInputQueue(t *testing.T) {
	r, err := fakeRedis()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	st := NewRedisStore(r.Addr(), testNamespace, "", 0)
	// st := NewRedisStore("localhost:6379", testNamespace, "", 0)
	c := st.pool.Get()
	defer c.Close()

	// Enqueue
	var specs []*TaskSpec
	const N = 5
	for i := 0; i < N; i++ {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		spec := specFromTask(&Task{Topic: "topic"})
		err = st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
		// Make them work
		out, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatalf("expected spec, got %v", out)
		}
	}
	if want, got := N, len(specs); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := N, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, deadQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}

	// Now move one from the worker queue to the dead queue
	err = st.MoveWorkQueueToInputQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := N, inputQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, workQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
	if want, got := 0, deadQSize(t, st, c); want != got {
		t.Fatalf("want %d, got %d", want, got)
	}
}
