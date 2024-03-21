package redis_queue

import "github.com/redis/go-redis/v9"

type Config struct {
	Redis *redis.UniversalOptions

	Queue struct {
		Name  string
		Retry int
	}

	Safe struct {
		AckZSetName string
		TTL         int
	}
}
