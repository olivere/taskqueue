# High-level design

Setup:

1. Create a manager
2. Register topics and processors
3. Run the manager
   - Initialize store
   - Initialize workers
   - Pull crashed tasks from work queue back into input queue
   - Start poller

Enqueing a task:

1. Call Enqueue with a TaskSpec.
   - Add TaskSpec to input queue

Poller:

1. Check if there is a slot available in the worker queue
2. If no, wait until next poller run
3. Pick the task to execute next from the input queue
4. If no, wait until next poller run
5. Remove the task from the input queue and put it into the worker queue
6. Start worker with the task
7. Go to 1

Worker:

1. Execute the processor associated with the task
2. If the processor succeeds, remove the task from the worker queue and stop
3. If the processor fails, ...
   3a. If retries are still available, remove the task from the
       worker queue and put it back into the input queue
   3b. Otherwise, move the task from the input queue to the dead queue

# Redis

The underlying Redis store uses these data structures:

- <namespace>:tasks:<id>      (holds serialized JSON data of TaskSpec)
- <namespace>:queue:import    (set of <id>'s waiting to be run)
- <namespace>:queue:work      (sorted set of <id>'s waiting to be run) [1]
- <namespace>:queue:dead      (set of <id>'s that failed repeatedly)
- <namespace>:stats:<field>   (statistics about the given field via GET/SET/INCRBY)

[1] The queue is sorted by the TaskSpec.Priority field and initialized
    by the currently time (`time.Now().UnixNano()`). Fields with lower
    time (which are added earlier) are executed first.
