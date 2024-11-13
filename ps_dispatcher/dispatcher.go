package ps_dispatcher

import "github.com/redis/go-redis/v9"

type ProcessFunc func(msg *redis.Message) (interface{}, error)
