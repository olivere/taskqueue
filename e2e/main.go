package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/olivere/taskqueue"
)

func main() {
	var (
		concurrency     = flag.Int("c", 5, "maximum number of workers")
		fillTime        = flag.Duration("fill-time", 500*time.Millisecond, "max fill time")
		runTime         = flag.Duration("run-time", 500*time.Millisecond, "max run time")
		logInterval     = flag.Duration("log-interval", 1*time.Second, "log interval for stats")
		interval        = flag.Duration("poll-interval", 500*time.Millisecond, "poll interval")
		numRetries      = flag.Int("num-retries", 2, "number of retries per task")
		redis           = flag.String("redis", "localhost:6379", "Redis server")
		redisns         = flag.String("redis-namespace", "taskqueue_e2e", "Redis namespace")
		redisdb         = flag.Int("redis-db", 0, "Redis database")
		topicsList      = flag.String("topics", "a,b,c", "comma-separated list of topics")
		failureRate     = flag.Float64("failure-rate", 0.05, "failure rate [0.0,1.0]")
		shutdownTimeout = flag.Duration("shutdown-timeout", -1*time.Second, "timeout to wait after shutdown")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	store := taskqueue.NewRedisStore(*redis, *redisns, "", *redisdb)

	var options []taskqueue.ManagerOption
	options = append(options, taskqueue.SetStore(store))
	options = append(options, taskqueue.SetConcurrency(*concurrency))
	options = append(options, taskqueue.SetPollInterval(*interval))
	m := taskqueue.New(options...)

	topics := strings.SplitN(*topicsList, ",", -1)
	for _, topic := range topics {
		err := m.Register(topic, makeProcessor(topic, *failureRate, *runTime))
		if err != nil {
			log.Fatal(err)
		}
	}
	err := m.Start()
	if err != nil {
		log.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		errc <- enqueuer(m, topics, *fillTime, *numRetries)
	}()

	go logger(m, *logInterval)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		log.Printf("signal %v", fmt.Sprint(<-c))
		errc <- m.CloseWithTimeout(*shutdownTimeout)
	}()

	if err := <-errc; err != nil {
		log.Fatal(err)
	} else {
		log.Print("exiting")
	}
}

func enqueuer(m *taskqueue.Manager, topics []string, fillTime time.Duration, numRetries int) error {
	fillTimeNanos := fillTime.Nanoseconds()
	for {
		time.Sleep(time.Duration(rand.Int63n(fillTimeNanos)) * time.Nanosecond)
		topic := topics[rand.Intn(len(topics))]
		t := &taskqueue.Task{Topic: topic, NumRetries: numRetries}
		err := m.Enqueue(t)
		if err != nil {
			return err
		}
	}
}

func logger(m *taskqueue.Manager, d time.Duration) {
	t := time.NewTicker(d)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			ss, err := m.Stats()
			if err == nil {
				fmt.Printf("Q=%6d S=%6d R=%6d F=%6d C=%6d IQ=%4d WQ=%4d DQ=%4d\n",
					ss.Enqueued,
					ss.Started,
					ss.Retried,
					ss.Failed,
					ss.Completed,
					ss.InputQueueSize,
					ss.WorkQueueSize,
					ss.DeadQueueSize)
			}
		}
	}
}

func makeProcessor(topic string, failureRate float64, runTime time.Duration) taskqueue.Processor {
	runTimeNanos := runTime.Nanoseconds()
	return func(args ...interface{}) error {
		time.Sleep(time.Duration(rand.Int63n(runTimeNanos)) * time.Nanosecond)
		if rand.Float64() < failureRate {
			return errors.New("processor failed")
		}
		return nil
	}
}
