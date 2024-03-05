package redis_queue

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
}

type Queue interface {
	Pop() (Message, error)
	Push(payload string) error
}

// CommonQueue is a simple queue
type CommonQueue struct {
	Name string
	ctx  context.Context
	rdb  redisClient
}

func (q *CommonQueue) Pop() (Message, error) {
	result, err := pop.Run(q.ctx, q.rdb, []string{q.Name}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means no message
			return nil, nil
		}
		return nil, err
	}

	v, ok := result.(string)
	if !ok {
		return nil, ErrUnexpectedType
	}

	return NewCommonMessage(v), nil
}

func (q *CommonQueue) Push(payload string) error {
	_, err := push.Run(q.ctx, q.rdb, []string{q.Name}, payload).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means failed to push
			return ErrPushFailed
		}
		return err
	}
	return nil
}

type SafeQueue struct {
	Name    string
	AckName string
	ttl     int
	ctx     context.Context
	rdb     redisClient
}

func (q *SafeQueue) SafePop() (Message, error) {
	result, err := ackPop.Run(q.ctx, q.rdb, []string{q.Name, q.AckName}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means no message
			return nil, nil
		}
		return nil, err
	}

	casted, ok := result.([]interface{})
	if !ok {
		return nil, ErrUnexpectedType
	}

	array := make([]string, len(casted))
	for i, v := range casted {
		str, ok := v.(string)
		if !ok {
			return nil, ErrUnexpectedType
		}
		array[i] = str
	}

	return NewSafeMessage(array[1], array[0], q), nil
}

func (q *SafeQueue) Pop() (Message, error) {
	result, err := pop.Run(q.ctx, q.rdb, []string{q.Name}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means no message
			return nil, nil
		}
		return nil, err
	}

	v, ok := result.(string)
	if !ok {
		return nil, ErrUnexpectedType
	}

	return NewCommonMessage(v), nil
}

func (q *SafeQueue) Push(payload string) error {
	_, err := push.Run(q.ctx, q.rdb, []string{q.Name}, payload).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Lua returns false, which means failed to push
			return ErrPushFailed
		}
		return err
	}
	return nil
}
