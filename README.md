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

  cfg := redisqueue.NewConfig()
  cfg.Redis = client
  cfg.Queue.Name = "YOUR_QUEUE_NAME"
  cfg.Safe.AckZSetName = "YOUR_ACK_ZSET_NAME"
  
  queue, err := redisqueue.NewSafeQueue(cfg)
  if err != nil {
	  panic(err)
  }

  err = queue.Push("testPayload")
  msg, err := queue.SafePop()
  if msg == nil && err == nil {
    // No message
  }

  if err != nil {
    // Error
  }

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

## Time Complexity

### CommonQueue Methods

| Method          | Time Complexity |
|-----------------|-----------------|
| `Push(payload)` | **O(1)**        |
| `Pop()`         | **O(1)**        |

### SafeQueue Methods

| Method          | Time Complexity |
|-----------------|-----------------|
| `Push(payload)` | **O(1)**        |
| `Pop()`         | **O(1)**        |
| `SafePop()`     | **O(log N)**    |
| `Ack()`         | **O(log N)**    |
