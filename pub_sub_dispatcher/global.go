package pub_sub_dispatcher

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
)

var (
	dispatcherMap = sync.Map{} //make(map[string]*RedisDispatcher)
)

func GetRedisDispatcher(alias string) (*RedisDispatcher, error) {
	if dispatchLoad, ok := dispatcherMap.Load(alias); ok && dispatchLoad.(*RedisDispatcher).Ctx.Err() == nil {
		return dispatchLoad.(*RedisDispatcher), nil
	}
	return nil, fmt.Errorf("pubsubDispatcher:GetRedisDispatcher dispatcher not fund")
}

func RegisterRedisDispatcher(ctx context.Context, alias string, redisClient *redis.Client) (rd *RedisDispatcher, err error) {
	if _, ok := dispatcherMap.Load(alias); ok {
		return nil, fmt.Errorf("pubsubDispatcher:RegisterRedisDispatcher fail. alias: %s has allready used. ", alias)
	}
	dispatcherMap.Store(alias, &RedisDispatcher{})
	defer func() {
		if err != nil {
			dispatcherMap.Delete(alias)
		}
		if dispatcherLoad, ok := dispatcherMap.Load(alias); !ok || dispatcherLoad.(*RedisDispatcher).Ctx.Err() != nil {
			dispatcherMap.Delete(alias)
		}
	}()
	rd = NewRedisDispatcher(ctx, redisClient)
	dispatcherMap.Store(alias, rd)
	return rd, nil
}
