package batch_operate

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type OpType uint8

var Debug = false

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

type OperateItem struct {
	OpType
	Key     string
	ExpTime time.Duration
	Args    interface{}
}

var operatePool = sync.Pool{
	New: func() interface{} {
		return &OperateItem{}
	},
}

func GetOperate() *OperateItem {
	return operatePool.Get().(*OperateItem)
}

func PutOperate(op *OperateItem) {
	op.OpType = 0
	op.Key = ""
	op.ExpTime = 0
	op.Args = nil
	operatePool.Put(op)
}

type BatchOperate struct {
	ctx           context.Context
	ticker        *time.Ticker
	maxLen        int
	redisCli      redis.UniversalClient
	batchChan     chan *OperateItem
	commitChannel chan struct{}
	cacheLen      int
}

func NewBatchOperate(ctx context.Context, redisCli redis.UniversalClient, maxLen int, duration time.Duration) *BatchOperate {
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
		batchChan:     make(chan *OperateItem, 10000),
		cacheLen:      0,
	}
	go batchOperate.Start()
	return batchOperate
}

func (bo *BatchOperate) Start() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	bo.cacheLen = 0
	pipe := bo.redisCli.Pipeline()
	for {
		select {
		case <-bo.ctx.Done():
			if bo.cacheLen > 0 {
				if Debug {
					startTime := time.Now()
					cmds, _ := pipe.Exec(context.Background())
					log.Printf("batchOperate ctx done took %s, length: %d, msg: %+v\n", time.Now().Sub(startTime), bo.cacheLen, cmds)
				} else {
					_, _ = pipe.Exec(context.Background())
				}
			}
			bo.ticker.Stop()
			return
		case <-bo.ticker.C:
			if bo.cacheLen > 0 {
				if Debug {
					startTime := time.Now()
					cmds, _ := pipe.Exec(bo.ctx)
					log.Printf("batchOperate ticker exec took %s, length: %d, msg:%+v\n", time.Now().Sub(startTime), bo.cacheLen, cmds)
				} else {
					_, _ = pipe.Exec(bo.ctx)
				}
				bo.cacheLen = 0
			}
		case <-bo.commitChannel:
			if bo.cacheLen > 0 {
				if Debug {
					startTime := time.Now()
					cmds, _ := pipe.Exec(bo.ctx)
					log.Printf("batchOperate receive commit command exec took %s, length: %d, msg:%+v\n", time.Now().Sub(startTime), bo.cacheLen, cmds)
				} else {
					_, _ = pipe.Exec(bo.ctx)
				}
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
						pipe.ZAdd(bo.ctx, op.Key, redis.Z{Score: f, Member: values[1]})
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
			PutOperate(op)
			if bo.cacheLen >= bo.maxLen {
				if Debug {
					startTime := time.Now()
					cmds, _ := pipe.Exec(bo.ctx)
					log.Printf("batchOperate over max len exec took %s, length: %d, msg: %+v\n", time.Now().Sub(startTime), bo.cacheLen, cmds)
				} else {
					_, _ = pipe.Exec(bo.ctx)
				}
				bo.cacheLen = 0
			}
		}
	}
}

func (bo *BatchOperate) Commit() {
	bo.commitChannel <- struct{}{}
}

func (bo *BatchOperate) Set(key string, value interface{}, expiration time.Duration) {
	op := GetOperate()
	op.OpType = Set
	op.Key = key
	op.ExpTime = expiration
	op.Args = value
	bo.batchChan <- op
}

func (bo *BatchOperate) HSet(key string, values ...interface{}) {
	op := GetOperate()
	op.OpType = HSet
	op.Key = key
	op.Args = values
	bo.batchChan <- op
}

func (bo *BatchOperate) HMSet(key string, values ...interface{}) {
	op := GetOperate()
	op.OpType = HMSet
	op.Key = key
	op.Args = values
	bo.batchChan <- op
}

func (bo *BatchOperate) HDel(key string, values ...string) {
	op := GetOperate()
	op.OpType = HDel
	op.Key = key
	op.Args = values
	bo.batchChan <- op
}

func (bo *BatchOperate) Publish(channel string, message interface{}) {
	op := GetOperate()
	op.OpType = Pub
	op.Key = channel
	op.Args = message
	bo.batchChan <- op
}

func (bo *BatchOperate) LPush(key string, values ...interface{}) {
	op := GetOperate()
	op.OpType = LPush
	op.Key = key
	op.Args = values
	bo.batchChan <- op
}

func (bo *BatchOperate) RPush(key string, values ...interface{}) {
	op := GetOperate()
	op.OpType = RPush
	op.Key = key
	op.Args = values
	bo.batchChan <- op
}

func (bo *BatchOperate) Expire(key string, expiration time.Duration) {
	op := GetOperate()
	op.OpType = Exp
	op.Key = key
	op.ExpTime = expiration
	bo.batchChan <- op
}

func (bo *BatchOperate) Del(key string) {
	op := GetOperate()
	op.OpType = Del
	op.Key = key
	bo.batchChan <- op
}

func (bo *BatchOperate) ZAdd(key string, member interface{}, score float64) {
	op := GetOperate()
	op.OpType = ZAdd
	op.Key = key
	op.Args = []interface{}{score, member}
	bo.batchChan <- op
}

func (bo *BatchOperate) ZRem(key string, member ...interface{}) {
	op := GetOperate()
	op.OpType = ZRem
	op.Key = key
	op.Args = member
	bo.batchChan <- op
}

func (bo *BatchOperate) Incr(key string, delta float64) {
	op := GetOperate()
	op.OpType = Incr
	op.Key = key
	op.Args = delta
	bo.batchChan <- op
}

func (bo *BatchOperate) SAdd(key string, member ...interface{}) {
	op := GetOperate()
	op.OpType = SAdd
	op.Key = key
	op.Args = member
	bo.batchChan <- op
}

func (bo *BatchOperate) SRem(key string, member ...interface{}) {
	op := GetOperate()
	op.OpType = SRem
	op.Key = key
	op.Args = member
	bo.batchChan <- op
}

func (bo *BatchOperate) SetTicker(duration time.Duration) {
	bo.ticker.Reset(duration)
}
