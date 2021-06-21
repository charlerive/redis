package batch_operate

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type OpType uint8

const (
	Set   OpType = 1
	HSet  OpType = 2
	HMSet OpType = 3
	Pub   OpType = 4
	LPush OpType = 5
	RPush OpType = 6
	Exp   OpType = 7
)

type Operate struct {
	OpType
	Key     string
	ExpTime time.Duration
	Args    interface{}
}

type BatchOperate struct {
	ctx       context.Context
	ticker    *time.Ticker
	maxLen    int
	redisCli  *redis.Client
	batchChan chan *Operate
}

func NewBatchOperate(ctx context.Context, redisCli *redis.Client, maxLen int, duration time.Duration) *BatchOperate {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxLen > 1000 {
		maxLen = 1000
	}
	batchOperate := &BatchOperate{
		ctx:       ctx,
		ticker:    time.NewTicker(duration),
		maxLen:    maxLen,
		redisCli:  redisCli,
		batchChan: make(chan *Operate, 5000),
	}
	go batchOperate.Start()
	return batchOperate
}

func (bo *BatchOperate) Start() {
	cacheLen := 0
	pipe := bo.redisCli.Pipeline()
	for {
		select {
		case <-bo.ctx.Done():
			if cacheLen > 0 {
				_, _ = pipe.Exec(bo.ctx)
			}
			return
		case <-bo.ticker.C:
			if cacheLen > 0 {
				_, _ = pipe.Exec(bo.ctx)
				pipe = bo.redisCli.Pipeline()
				cacheLen = 0
			}
		case op := <-bo.batchChan:
			cacheLen++
			switch op.OpType {
			case Set:
				pipe.Set(bo.ctx, op.Key, op.Args, op.ExpTime)
			case HSet:
				values := op.Args.([]interface{})
				pipe.HSet(bo.ctx, op.Key, values...)
			case HMSet:
				values := op.Args.([]interface{})
				pipe.HMSet(bo.ctx, op.Key, values...)
			case Pub:
				pipe.Publish(bo.ctx, op.Key, op.Args)
			case LPush:
				values := op.Args.([]interface{})
				pipe.LPush(bo.ctx, op.Key, values...)
			case RPush:
				values := op.Args.([]interface{})
				pipe.RPush(bo.ctx, op.Key, values...)
			case Exp:
				pipe.Expire(bo.ctx, op.Key, op.ExpTime)
			}
			if cacheLen >= bo.maxLen {
				_, _ = pipe.Exec(bo.ctx)
				pipe = bo.redisCli.Pipeline()
				cacheLen = 0
			}
		}
	}
}

func (bo *BatchOperate) Set(key string, value interface{}, expiration time.Duration) {
	op := &Operate{
		OpType:  Set,
		Key:     key,
		ExpTime: expiration,
		Args:    value,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) HSet(key string, values ...interface{}) {
	op := &Operate{
		OpType: HSet,
		Key:    key,
		Args:   values,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) HMSet(key string, values ...interface{}) {
	op := &Operate{
		OpType: HMSet,
		Key:    key,
		Args:   values,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) Publish(channel string, message interface{}) {
	op := &Operate{
		OpType: Pub,
		Key:    channel,
		Args:   message,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) LPush(key string, values ...interface{}) {
	op := &Operate{
		OpType: LPush,
		Key:    key,
		Args:   values,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) RPush(key string, values ...interface{}) {
	op := &Operate{
		OpType: RPush,
		Key:    key,
		Args:   values,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) Expire(key string, expiration time.Duration) {
	op := &Operate{
		OpType:  Exp,
		Key:     key,
		ExpTime: expiration,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) SetTicker(duration time.Duration) {
	bo.ticker.Reset(duration)
}
