// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	defaultRedisServer = ":6379"
)

type RedisStore struct {
	pool *redis.Pool
	ns   string
}

func NewRedisStore(server, namespace, password string, db int) *RedisStore {
	return NewRedisStoreFromPool(namespace, newPool(server, password, db))
}

func NewRedisStoreFromPool(namespace string, pool *redis.Pool) *RedisStore {
	return &RedisStore{
		ns:   namespace,
		pool: pool,
	}
}

// newPool creates a new Redis pool with sane defaults.
func newPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5, // pool size
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			_, err = c.Do("SELECT", db)
			if err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (r *RedisStore) key(keys ...string) string {
	if r.ns == "" {
		return strings.Join(keys, ":")
	}
	return strings.Join([]string{r.ns, strings.Join(keys, ":")}, ":")
}

// Enqueue adds the task specification to the input queue.
func (r *RedisStore) Enqueue(spec *TaskSpec) error {
	c := r.pool.Get()
	defer c.Close()

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	score := fmt.Sprintf("%d", spec.Priority)

	c.Send("MULTI")
	c.Send("SET", r.key("tasks", spec.ID), string(data))
	c.Send("ZADD", r.key("queue", "input"), "NX", score, spec.ID)
	_, err = c.Do("EXEC") // should send ["OK", "1"]
	if err != nil {
		c.Do("DISCARD")
		return err
	}
	return nil
}

// Dequeue removes the task specification from the work queue.
func (r *RedisStore) Dequeue(id string) error {
	c := r.pool.Get()
	defer c.Close()

	c.Send("MULTI")
	c.Send("SREM", r.key("queue", "work"), id)
	c.Send("DEL", r.key("tasks", id))
	_, err := c.Do("EXEC") // should send ["1", "1"]
	if err != nil {
		c.Do("DISCARD")
		return err
	}
	return nil
}

// Next checks for tasks in the input queue and, if one is found, moves it
// into the work queue. If there are no tasks in the input queue,
// nil is returned.
func (r *RedisStore) Next() (*TaskSpec, error) {
	c := r.pool.Get()
	defer c.Close()

	// KEYS[1] name of input queue (queue:input)
	// KEYS[2] name of work queue (queue:work)
	// KEYS[3] prefix of tasks hash (tasks)
	var script = redis.NewScript(3, `
		local r = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', '0', '1')
		if r ~= false and #r ~= 0 then
			local id = r[1]
			-- Remove from input queue
			local n = redis.call('ZREM', KEYS[1], id)
			if n == nil then
				return nil
			end
			-- Add to work queue
			local n = redis.call('SADD', KEYS[2], id)
			if n == nil then
				return nil
			end
			return redis.call('GET', KEYS[3] .. ':' .. id)
		end
		return ''
	`)

	inputQ := r.key("queue", "input")
	workQ := r.key("queue", "work")
	tasks := r.key("tasks")

	v, err := redis.String(script.Do(c, inputQ, workQ, tasks))
	if err != nil {
		return nil, err
	}
	if v == "" {
		return nil, nil
	}
	var spec TaskSpec
	if err := json.Unmarshal([]byte(v), &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Retry takes a task specification and moves it from the work queue
// back into the input queue.
func (r *RedisStore) Retry(spec *TaskSpec) error {
	c := r.pool.Get()
	defer c.Close()

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// KEYS[1] = Spec ID
	// KEYS[2] = Serialized spec data
	// KEYS[3] = Name of input queue (queue:input)
	// KEYS[4] = Name of work queue (queue:work)
	// KEYS[5] = Name of tasks entry (tasks:<id>)
	// KEYS[6] = Sort key
	var script = redis.NewScript(6, `
		local r = redis.call('SREM', KEYS[4], KEYS[1])
		if r == 1 then
			redis.call('SET', KEYS[5], KEYS[2])
			redis.call('ZADD', KEYS[3], 'NX', KEYS[6], KEYS[1])
		end
		return r
	`)

	// Enqueue later, no?
	score := fmt.Sprintf("%d", spec.Priority)
	inputQ := r.key("queue", "input")
	workQ := r.key("queue", "work")
	taskKey := r.key("tasks", spec.ID)

	_, err = redis.Int(script.Do(c, spec.ID, data, inputQ, workQ, taskKey, score))
	if err != nil {
		return err
	}
	return nil
}

// MoveToDeadQueue moves the task from the work queue to the dead queue.
func (r *RedisStore) MoveToDeadQueue(spec *TaskSpec) error {
	c := r.pool.Get()
	defer c.Close()

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// KEYS[1] = Spec ID
	// KEYS[2] = Serialized spec data
	// KEYS[3] = Name of dead queue (queue:dead)
	// KEYS[4] = Name of work queue (queue:work)
	// KEYS[5] = Name of tasks entry (tasks:<id>)
	var script = redis.NewScript(5, `
		local r = redis.call('SREM', KEYS[4], KEYS[1])
		if r == 1 then
			redis.call('SET', KEYS[5], KEYS[2])
			redis.call('SADD', KEYS[3], KEYS[1])
		end
		return r
	`)

	// Enqueue later, no?
	deadQ := r.key("queue", "dead")
	workQ := r.key("queue", "work")
	taskKey := r.key("tasks", spec.ID)

	_, err = redis.Int(script.Do(c, spec.ID, data, deadQ, workQ, taskKey))
	if err != nil {
		return err
	}
	return nil
}

// Move all items from work queue to input queue (used at startup).
func (r *RedisStore) MoveWorkQueueToInputQueue() error {
	c := r.pool.Get()
	defer c.Close()

	// KEYS[1] = Name of input queue (queue:input)
	// KEYS[2] = Name of work queue (queue:work)
	// KEYS[3] = Name of tasks queue (tasks)
	// KEYS[4] = Score
	var script = redis.NewScript(4, `
		local n = 0
		local ids = redis.call('SMEMBERS', KEYS[2])
		for _, id in pairs(ids) do
			redis.call('SREM', KEYS[2], id)
			local task = redis.call('GET', KEYS[3] .. ':' .. id)
			if task ~= nil then
				redis.call('ZADD', KEYS[1], 'NX', KEYS[4], id)
			end
		end
		return n
	`)

	// Enqueue later, no?
	inputQ := r.key("queue", "input")
	workQ := r.key("queue", "work")
	tasksQ := r.key("tasks")
	sortKey := fmt.Sprintf("%d", time.Now().UnixNano())

	_, err := redis.Int(script.Do(c, inputQ, workQ, tasksQ, sortKey))
	if err != nil {
		return err
	}
	return nil
}

// SizeOfInputQueue returns the current number of items in the input queue.
func (r *RedisStore) SizeOfInputQueue() (int, error) {
	c := r.pool.Get()
	defer c.Close()
	return redis.Int(c.Do("ZCARD", r.key("queue", "input")))
}

// SizeOfWorkQueue returns the current number of items in the work queue.
func (r *RedisStore) SizeOfWorkQueue() (int, error) {
	c := r.pool.Get()
	defer c.Close()
	return redis.Int(c.Do("SCARD", r.key("queue", "work")))
}

// SizeOfDeadQueue returns the current number of items in the dead queue.
func (r *RedisStore) SizeOfDeadQueue() (int, error) {
	c := r.pool.Get()
	defer c.Close()
	return redis.Int(c.Do("SCARD", r.key("queue", "dead")))
}

// StatsSnapshot reads the stored statistics.
func (r *RedisStore) StatsSnapshot() (*Stats, error) {
	c := r.pool.Get()
	defer c.Close()

	v, err := redis.Ints(c.Do("MGET",
		r.key("stats", "enqueued"),
		r.key("stats", "started"),
		r.key("stats", "retried"),
		r.key("stats", "failed"),
		r.key("stats", "completed"),
	))
	if err != nil {
		return nil, err
	}
	if len(v) != 5 {
		return nil, err
	}
	st := new(Stats)
	st.Enqueued = v[0]
	st.Started = v[1]
	st.Retried = v[2]
	st.Failed = v[3]
	st.Completed = v[4]
	n, _ := r.SizeOfInputQueue()
	st.InputQueueSize = n
	n, _ = r.SizeOfWorkQueue()
	st.WorkQueueSize = n
	n, _ = r.SizeOfDeadQueue()
	st.DeadQueueSize = n
	return st, nil
}

// StatsIncrement writes the updated statistics to the store.
func (r *RedisStore) StatsIncrement(f StatsField, delta int) error {
	c := r.pool.Get()
	defer c.Close()

	var key string
	switch f {
	case EnqueuedField:
		key = r.key("stats", "enqueued")
	case StartedField:
		key = r.key("stats", "started")
	case RetriedField:
		key = r.key("stats", "retried")
	case FailedField:
		key = r.key("stats", "failed")
	case CompletedField:
		key = r.key("stats", "completed")
	default:
		return nil
	}
	_, err := c.Do("INCRBY", key, delta)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Publish(e *WatchEvent) error {
	c := r.pool.Get()
	defer c.Close()
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = c.Do("PUBLISH", r.key("events"), string(data))
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Subscribe(recv chan *WatchEvent) {
	events := make(chan *WatchEvent)
	defer close(events)
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			c := r.pool.Get()
			defer c.Close()
			psc := redis.PubSubConn{Conn: c}
			psc.Subscribe(r.key("events"))

			for c.Err() == nil {
				select {
				case <-events:
					return
				default:
					switch n := psc.Receive().(type) {
					case redis.Message:
						e := new(WatchEvent)
						if err := json.Unmarshal(n.Data, &e); err != nil {
							break
						}
						events <- e
					case redis.Subscription:
						break
					case error:
						break
					}
				}
			}
		}
	}()

	for {
		select {
		case <-recv:
			wg.Wait()
			return
		case e := <-events:
			recv <- e
		}
	}
}
