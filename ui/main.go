package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/olivere/taskqueue"
	"github.com/olivere/taskqueue/ui/server"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:12345", "HTTP bind address")
		redis   = flag.String("redis", "localhost:6379", "Redis server")
		redisns = flag.String("redis-namespace", "taskqueue_e2e", "Redis namespace")
		redisdb = flag.Int("redis-db", 0, "Redis database")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	logger := log.NewJSONLogger(os.Stdout)
	logger = log.NewContext(logger).With("t", log.DefaultTimestamp)

	store := taskqueue.NewRedisStore(*redis, *redisns, "", *redisdb)

	var options []taskqueue.ManagerOption
	options = append(options, taskqueue.SetStore(store))
	options = append(options, taskqueue.SetLogger(wrapKitLogger{logger}))
	m := taskqueue.New(options...)
	defer m.Close()

	errc := make(chan error, 1)

	go func() {
		logger.Log("msg", "web server started", "addr", *addr)
		s := server.New(logger, m)
		errc <- s.Serve(*addr)
	}()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		logger.Log("signal", fmt.Sprint(<-c))
		errc <- nil
	}()

	if err := <-errc; err != nil {
		logger.Log("err", err)
		os.Exit(1)
	} else {
		logger.Log("msg", "exiting")
	}
}

type wrapKitLogger struct {
	log.Logger
}

func (logger wrapKitLogger) Printf(format string, v ...interface{}) {
	logger.Log("msg", fmt.Sprintf(format, v...))
}
