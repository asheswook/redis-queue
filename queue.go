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

func NewCommonQueue(cfg *Config) *CommonQueue {
	return &CommonQueue{
		Name:  cfg.Queue.Name,
		ctx:   context.Background(),
		rdb:   redis.NewUniversalClient(cfg.Redis),
		retry: cfg.Queue.Retry,
	}
}

// CommonQueue is a simple queue
type CommonQueue struct {
	Name  string
	ctx   context.Context
	rdb   redisClient
	retry int
}

func (q *CommonQueue) pop() (Message, error) {
	result, err := pop.Run(q.ctx, q.rdb, []string{q.Name}).Result()
	if err != nil {
		return nil, err
	}

	v, ok := result.(string)
	if !ok {
		return nil, ErrUnexpectedType
	}

	return NewCommonMessage(v), nil
}

func (q *CommonQueue) Pop() (msg Message, err error) {
	msg, err = q.pop()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil
	case err != nil:
		return nil, err
	}

	return msg, nil
}

func (q *CommonQueue) push(payload string) error {
	_, err := push.Run(q.ctx, q.rdb, []string{q.Name}, payload).Result()
	if err != nil {
		return err
	}
	return nil
}

func (q *CommonQueue) Push(payload string) (err error) {
	err = q.push(payload)
	switch {
	case errors.Is(err, redis.Nil):
		return ErrPushFailed
	case err != nil:
		return err
	}

	return nil
}

func NewSafeQueue(cfg *Config) *SafeQueue {
	return &SafeQueue{
		Name:    cfg.Queue.Name,
		AckName: cfg.Safe.AckZSetName,
		ttl:     cfg.Safe.TTL,
		ctx:     context.Background(),
		rdb:     redis.NewUniversalClient(cfg.Redis),
		retry:   cfg.Queue.Retry,
	}
}

type SafeQueue struct {
	Name    string
	AckName string
	ttl     int
	ctx     context.Context
	rdb     redisClient
	retry   int
}

func (q *SafeQueue) safePop() (*SafeMessage, error) {
	result, err := ackPop.Run(q.ctx, q.rdb, []string{q.Name, q.AckName}, q.ttl).Result()
	if err != nil {
		return nil, err
	}

	v, ok := result.(string)
	if !ok {
		return nil, ErrUnexpectedType
	}

	return NewSafeMessage(v, q), nil
}

func (q *SafeQueue) SafePop() (msg *SafeMessage, err error) {
	for i := 0; i < q.retry; i++ {
		msg, err = q.safePop()
		switch {
		case errors.Is(err, redis.Nil):
			return nil, nil
		case errors.Is(err, ErrTimestampUpdateFailed):
			continue
		case errors.Is(err, ErrAckPopFailed):
			continue
		case err != nil:
			return nil, err
		}

		// No error, return
		break
	}
	return msg, nil
}

func (q *SafeQueue) pop() (Message, error) {
	result, err := pop.Run(q.ctx, q.rdb, []string{q.Name}).Result()
	if err != nil {
		return nil, err
	}

	v, ok := result.(string)
	if !ok {
		return nil, ErrUnexpectedType
	}

	return NewCommonMessage(v), nil
}

func (q *SafeQueue) Pop() (msg Message, err error) {
	msg, err = q.pop()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil
	case err != nil:
		return nil, err
	}

	return msg, nil
}

func (q *SafeQueue) push(payload string) error {
	_, err := push.Run(q.ctx, q.rdb, []string{q.Name}, payload).Result()
	if err != nil {
		return err
	}
	return nil
}

func (q *SafeQueue) Push(payload string) (err error) {
	err = q.push(payload)
	switch {
	case errors.Is(err, redis.Nil):
		return ErrPushFailed
	case err != nil:
		return err
	}

	return nil
}
