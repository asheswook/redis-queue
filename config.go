package redis_queue

import "fmt"

// Config holds the configuration for redis-queue.
type Config struct {
	// Redis is the go-redis client implementation (e.g., *redis.Client or *redis.ClusterClient).
	Redis redisClient

	// Queue contains the main queue configuration
	Queue struct {
		// Name is the Redis key name for the queue
		Name string
		// Retry is the number of retry attempts for SafePop operations when errors occur
		// (e.g., timestamp update failed, ack pop failed). Default is 3.
		Retry int
	}

	// Safe contains configuration specific to SafeQueue
	Safe struct {
		// AckZSetName is the Redis ZSet key name for tracking unacknowledged messages
		AckZSetName string
		// TTL is the time-to-live in seconds for unacknowledged messages.
		// If a message is not acknowledged within this duration, it will be available
		// for re-processing. Default is 10 seconds.
		TTL int
	}
}

func (c *Config) isValid() error {
	if c.Queue.Name == "" {
		return fmt.Errorf("%s, Queue.Name must not be empty", ErrConfigInvalid)
	}
	if c.Safe.AckZSetName == "" {
		return fmt.Errorf("%s, AckZSetName must not be empty", ErrConfigInvalid)
	}
	if c.Safe.TTL <= 0 {
		return fmt.Errorf("%s, TTL must be greater than 0", ErrConfigInvalid)
	}
	if c.Queue.Retry <= 0 {
		return fmt.Errorf("%s, Retry must be greater than 0", ErrConfigInvalid)
	}
	if c.Redis == nil {
		return fmt.Errorf("%s, Redis must not be nil", ErrConfigInvalid)
	}
	if c.Queue.Name == c.Safe.AckZSetName {
		return fmt.Errorf("%s, Queue.Name and Safe.AckZSetName must be different", ErrConfigInvalid)
	}
	return nil
}

func NewConfig() *Config {
	c := &Config{}
	c.Redis = nil
	c.Queue.Name = ""
	c.Queue.Retry = 3
	c.Safe.AckZSetName = ""
	c.Safe.TTL = 10
	return c
}
