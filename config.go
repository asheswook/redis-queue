package redis_queue

type Config struct {
	Redis redisClient

	Queue struct {
		Name  string
		Retry int
	}

	Safe struct {
		AckZSetName string
		TTL         int
	}
}
