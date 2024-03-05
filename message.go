package redis_queue

import (
	"errors"
	"github.com/redis/go-redis/v9"
)

type Message interface {
	Payload() string
}

func NewCommonMessage(payload string) *CommonMessage {
	return &CommonMessage{payload: payload}
}

func NewSafeMessage(payload string, prefix string, queue *SafeQueue) *SafeMessage {
	return &SafeMessage{payload: payload, prefix: prefix, queue: queue}
}

// CommonMessage is a simple message
type CommonMessage struct {
	payload string
}

func (msg *CommonMessage) Payload() string {
	return msg.payload
}

// SafeMessage is a message that can be acked.
// When you receive a message from SafePop, you can ack it.
// If you don't ack it, the message will be popped again after a while.
type SafeMessage struct {
	payload string
	prefix  string
	queue   *SafeQueue
}

func (msg *SafeMessage) Payload() string {
	return msg.payload
}

func (msg *SafeMessage) Ack() error {
	_, err := ack.Run(msg.queue.ctx, msg.queue.rdb, []string{msg.queue.AckName}, msg.value()).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means failed to ack
			return ErrAckFailed
		}
		return err
	}

	return nil
}

func (msg *SafeMessage) value() string {
	return msg.prefix + msg.payload
}
