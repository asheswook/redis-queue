package redis_queue

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var ctx = context.Background()

func ClearDatabase(rdb redisClient, ctx context.Context, key string) {
	rdb.Eval(ctx, `redis.call('DEL', KEYS[1])`, []string{key})
}

func TestCommonQueue(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	Name := "test"

	queue := CommonQueue{
		Name:  Name,
		ctx:   ctx,
		rdb:   client,
		retry: 3,
	}

	// Clear test queue
	ClearDatabase(queue.rdb, queue.ctx, Name)

	var err error
	var msg Message

	t.Run("PushAndPop", func(t *testing.T) {
		err = queue.Push("testPayload")
		assert.Nil(t, err)

		msg, err = queue.Pop()
		assert.Nil(t, err)

		assert.Equal(t, "testPayload", msg.Payload())

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Nil(t, msg)

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Nil(t, msg)

		err = queue.Push("testPayload1")
		assert.Nil(t, err)

		err = queue.Push("testPayload2")
		assert.Nil(t, err)

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload1", msg.Payload())

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload2", msg.Payload())
	})
}

func TestSafeQueue(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	Name := "test"
	ackName := "ack:test"

	queue := SafeQueue{
		Name:    Name,
		AckName: ackName,
		ttl:     1,
		ctx:     ctx,
		rdb:     client,
		retry:   3,
	}

	var err error
	var msg Message

	// Clear test queue
	ClearDatabase(queue.rdb, queue.ctx, Name)

	// Clear ack zset
	ClearDatabase(queue.rdb, queue.ctx, ackName)

	t.Run("PushAndPop", func(t *testing.T) {
		err = queue.Push("testPayload")
		assert.Nil(t, err)

		msg, err = queue.Pop()
		assert.Nil(t, err)

		assert.Equal(t, "testPayload", msg.Payload())

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Nil(t, msg)

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Nil(t, msg)

		err = queue.Push("testPayload1")
		assert.Nil(t, err)

		err = queue.Push("testPayload2")
		assert.Nil(t, err)

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload1", msg.Payload())

		msg, err = queue.Pop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload2", msg.Payload())
	})

	var safeMsg *SafeMessage

	t.Run("PushAndSafePop", func(t *testing.T) {
		err = queue.Push("testPayload")
		assert.Nil(t, err)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload", safeMsg.Payload())

		err = safeMsg.Ack()
		assert.Nil(t, err)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Nil(t, safeMsg)

		err = queue.Push("testPayload1")
		assert.Nil(t, err)

		err = queue.Push("testPayload2")
		assert.Nil(t, err)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload1", safeMsg.Payload())

		time.Sleep(2 * time.Second)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload1", safeMsg.Payload())

		err = safeMsg.Ack()
		assert.Nil(t, err)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Equal(t, "testPayload2", safeMsg.Payload())

		err = safeMsg.Ack()
		assert.Nil(t, err)

		safeMsg, err = queue.SafePop()
		assert.Nil(t, err)
		assert.Nil(t, safeMsg)
	})
}
