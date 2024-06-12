package batch_operate

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type OpType uint8

const (
	Set               OpType = 1
	HSet              OpType = 2
	HMSet             OpType = 3
	HDel              OpType = 4
	Pub               OpType = 5
	LPush             OpType = 6
	RPush             OpType = 7
	Exp               OpType = 8
	Del               OpType = 9
	ZAdd              OpType = 10
	ZRem              OpType = 11
	Incr              OpType = 12
	SAdd              OpType = 13
	SRem              OpType = 14
	PipelineMaxLength        = 10000
)

type Operate struct {
	OpType
	Key     string
	ExpTime time.Duration
	Args    interface{}
}

type BatchOperate struct {
	ctx           context.Context
	ticker        *time.Ticker
	maxLen        int
	redisCli      *redis.Client
	batchChan     chan *Operate
	commitChannel chan struct{}
	cacheLen      int
}

func NewBatchOperate(ctx context.Context, redisCli *redis.Client, maxLen int, duration time.Duration) *BatchOperate {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxLen > PipelineMaxLength {
		maxLen = PipelineMaxLength
	}
	batchOperate := &BatchOperate{
		ctx:           ctx,
		ticker:        time.NewTicker(duration),
		maxLen:        maxLen,
		redisCli:      redisCli,
		commitChannel: make(chan struct{}, 100),
		batchChan:     make(chan *Operate, 10000),
		cacheLen:      0,
	}
	go batchOperate.Start()
	return batchOperate
}

func (bo *BatchOperate) Start() {
	bo.cacheLen = 0
	pipe := bo.redisCli.Pipeline()
	for {
		select {
		case <-bo.ctx.Done():
			if bo.cacheLen > 0 {
				_, _ = pipe.Exec(context.Background())
			}
			bo.ticker.Stop()
			return
		case <-bo.ticker.C:
			if bo.cacheLen > 0 {
				_, _ = pipe.Exec(bo.ctx)
				bo.cacheLen = 0
			}
		case <-bo.commitChannel:
			if bo.cacheLen > 0 {
				_, _ = pipe.Exec(bo.ctx)
				bo.cacheLen = 0
			}
		case op := <-bo.batchChan:
			bo.cacheLen++
			switch op.OpType {
			case Set:
				pipe.Set(bo.ctx, op.Key, op.Args, op.ExpTime)
			case HSet:
				values := op.Args.([]interface{})
				pipe.HSet(bo.ctx, op.Key, values...)
			case HMSet:
				values := op.Args.([]interface{})
				pipe.HMSet(bo.ctx, op.Key, values...)
			case HDel:
				values := op.Args.([]string)
				pipe.HDel(bo.ctx, op.Key, values...)
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
			case Del:
				pipe.Del(bo.ctx, op.Key)
			case ZAdd:
				values := op.Args.([]interface{})
				if len(values) == 2 {
					if f, ok := values[0].(float64); ok {
						pipe.ZAdd(bo.ctx, op.Key, &redis.Z{Score: f, Member: values[1]})
					}
				}
			case ZRem:
				values := op.Args.([]interface{})
				pipe.ZRem(bo.ctx, op.Key, values...)
			case Incr:
				pipe.IncrByFloat(bo.ctx, op.Key, op.Args.(float64))
			case SAdd:
				values := op.Args.([]interface{})
				pipe.SAdd(bo.ctx, op.Key, values...)
			case SRem:
				values := op.Args.([]interface{})
				pipe.SRem(bo.ctx, op.Key, values...)
			}
			if bo.cacheLen >= bo.maxLen {
				_, _ = pipe.Exec(bo.ctx)
				bo.cacheLen = 0
			}
		}
	}
}

func (bo *BatchOperate) Commit() {
	bo.commitChannel <- struct{}{}
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

func (bo *BatchOperate) HDel(key string, values ...string) {
	op := &Operate{
		OpType: HDel,
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

func (bo *BatchOperate) Del(key string) {
	op := &Operate{
		OpType: Del,
		Key:    key,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) ZAdd(key string, member interface{}, score float64) {
	op := &Operate{
		OpType: ZAdd,
		Key:    key,
		Args:   []interface{}{score, member},
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) ZRem(key string, member ...interface{}) {
	op := &Operate{
		OpType: ZRem,
		Key:    key,
		Args:   member,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) Incr(key string, delta float64) {
	op := &Operate{
		OpType: Incr,
		Key:    key,
		Args:   delta,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) SAdd(key string, member ...interface{}) {
	op := &Operate{
		OpType: SAdd,
		Key:    key,
		Args:   member,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) SRem(key string, member ...interface{}) {
	op := &Operate{
		OpType: SRem,
		Key:    key,
		Args:   member,
	}
	bo.batchChan <- op
}

func (bo *BatchOperate) SetTicker(duration time.Duration) {
	bo.ticker.Reset(duration)
}
