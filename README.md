# redis-queue

a reliable, lightweight message queue for golang

# Feature
1. **Queue Interaction**: Enables push and pop interactions with the message queue.
2. **Atomic**: Handles push and pop operations atomically, providing safety against race conditions.
3. **Persistence**: Ensures no message loss, even in the event of an unexpected program termination.

# Usage
```
go get -u github.com/asheswook/redis-queue
```

```go
package main

import (
  "github.com/redis/go-redis/v9"
  redisqueue "github.com/asheswook/redis-queue"
)

func main() {
  // Using go-redis client
  // You can also use normal client instead of cluster client
  client := redis.NewClusterClient( 
    &redis.ClusterOptions{
      Addrs: config.Addrs,
    },
  )

  queue := redisqueue.NewSafeQueue(
    &redisqueue.Config{
      Redis: client,
      Queue: struct {
        Name  string
        Retry int
      }{
        Name:  fmt.Sprintf("{%s}", config.QueueName),
        Retry: config.Retry,
      },
      Safe: struct {
        AckZSetName string
        TTL         int
      }{
        AckZSetName: fmt.Sprintf("{%s}:ack", config.QueueName),
        TTL:         config.TTL,
      },
    },
  )

  err := queue.Push("testPayload")
  msg, err := queue.SafePop()

  // Signal the message has been processed successfully.
  _ = msg.Ack()
}
```

# How it works

<p align="center">
  <img src="https://github.com/asheswook/redis-queue/assets/25760310/e4f3cc1f-deea-4911-b64a-d308d6172e0c" width='700px' />
</p>
<p align="center">  
  <img src="https://github.com/asheswook/redis-queue/assets/25760310/07d08aea-9cca-48e2-a6f0-da8dc6663306" width='700px' />
</p>
