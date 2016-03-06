// Package taskqueue manages running and scheduling tasks.
//
// Applications using taskqueue first create a Manager. One manager handles
// one or more topics. There is one processor per topic. Applications need
// to register topics and their processors before starting the manager.
//
// After topics and processors are registered, applications can start
// the manager. The manager then initializes a list of workers that will
// work on the actual tasks. At the beginning, all workers are idle.
//
// The manager has a Store to implemented persistent storage. By default,
// Redis is used as a backend store. Internally, the manager has a list of
// three states and this resembles in Redis. There is an input queue which
// contains all the tasks that need to be worked on. Then there is a work
// queue that contains all tasks currently being worked on. Finally, there
// is a dead queue that contains all tasks that couldn't be completed, even
// after retrying. The dead queue will not be touched until a human will move
// tasks back into the input queue.
//
// After being started, the manager moves all tasks in the work queue back
// into the input queue. This could happen in the event that a previously
// started manager couldn't complete tasks e.g. because it has crashed.
// After that, the manager periodically polls the input queue for
// new tasks. When a new task is found and a worker is available, the
// manager finds the associated processor and puts it into the work queue,
// and passes it to a worker to run the task.
//
// If the worker finishes successfully, the task is removed from the work
// queue and disappears. If the worker fails, it is retried according to
// the NumRetries set when enqueing the task initially. Such tasks are
// simply put back into the input queue to be scheduled again at a later
// time. If a worker fails even after retrying, it is moved to the
// dead queue for inspection by a human.
package taskqueue
