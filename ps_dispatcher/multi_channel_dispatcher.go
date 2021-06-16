package ps_dispatcher

import (
	"bytes"
	"context"
	"github.com/go-redis/redis"
	"log"
	"sync"
	"time"
)

var (
	SubChannelChan   = make(chan string, 2000)
	UnsubChannelChan = make(chan string, 2000)
)

type MultiRedisMessageDispatcher struct {
	mu          sync.RWMutex
	pubChannel  chan *redis.Message
	subChannels sync.Map // map[string]struct{}
}

func (md *MultiRedisMessageDispatcher) Init() {
	md.pubChannel = make(chan *redis.Message, 100)
	md.subChannels = sync.Map{}
}

func (md *MultiRedisMessageDispatcher) Channel() chan *redis.Message {
	return md.pubChannel
}

func (md *MultiRedisMessageDispatcher) String() string {
	buf := bytes.Buffer{}
	md.subChannels.Range(func(channel, _ interface{}) bool {
		buf.WriteString(channel.(string))
		buf.WriteString(",")
		return true
	})
	channels := buf.String()
	return channels[:len(channels)-1]
}

func (md *MultiRedisMessageDispatcher) Subscribe(channels ...string) {
	for k, channel := range channels {
		if _, ok := md.subChannels.Load(channel); ok {
			channels = append(channels[0:k], channels[k+1:]...)
			continue
		}
		md.subChannels.Store(channel, struct{}{})
	}

	for _, s := range channels {
		SubChannelChan <- s
	}
}

func (md *MultiRedisMessageDispatcher) Unsubscribe(channels ...string) {
	for k, channel := range channels {
		if _, ok := md.subChannels.Load(channel); !ok {
			channels = append(channels[0:k], channels[k+1:]...)
			continue
		}
		md.subChannels.Delete(channel)
	}

	for _, s := range channels {
		UnsubChannelChan <- s
	}
}

func (md *MultiRedisMessageDispatcher) Close() {
	md.subChannels.Range(func(channel, _ interface{}) bool {
		UnsubChannelChan <- channel.(string)
		md.subChannels.Delete(channel)
		return true
	})
	close(md.pubChannel)
}

func (md *MultiRedisMessageDispatcher) pub(msg *redis.Message) {
	if _, ok := md.subChannels.Load(msg.Channel); ok {
		if len(md.pubChannel) < 90 {
			md.pubChannel <- msg
		}
		return
	}
}

type MultiRedisMessageDispatcherPool struct {
	ctx               context.Context
	redisClient       *redis.Client
	subChannels       map[string]int64
	redisSubMap       map[string]*redis.PubSub
	addDispatcherChan chan *MultiRedisMessageDispatcher
	delDispatcherChan chan *MultiRedisMessageDispatcher
	dispatcherList    []*MultiRedisMessageDispatcher
	mu                sync.Mutex

	subChannelsLen int64
	subCount       int64
	unsubCount     int64
	connectCount   int64
	breakCount     int64
}

func NewMultiRedisMessageDispatcherPool(ctx context.Context, redisClient *redis.Client) (mdp *MultiRedisMessageDispatcherPool) {
	if ctx == nil {
		ctx = context.Background()
	}
	mdp = &MultiRedisMessageDispatcherPool{
		ctx:               ctx,
		redisClient:       redisClient,
		subChannels:       make(map[string]int64),
		redisSubMap:       make(map[string]*redis.PubSub),
		addDispatcherChan: make(chan *MultiRedisMessageDispatcher, 100),
		delDispatcherChan: make(chan *MultiRedisMessageDispatcher, 100),
		dispatcherList:    make([]*MultiRedisMessageDispatcher, 0),
		mu:                sync.Mutex{},
	}

	go mdp.dealSubscribeRequest()
	go mdp.dealDispatcherRequest()

	return mdp
}

func (mdp *MultiRedisMessageDispatcherPool) PrintLength(duration time.Duration) {
	timer := time.NewTicker(duration)
	go func() {
		for {
			select {
			case <-mdp.ctx.Done():
				return
			case <-timer.C:
				log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:PrintLength subChannelsLen: %d, redisChanListLen: %d", mdp.subChannelsLen, len(mdp.dispatcherList))
				log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:PrintLength SubCount: %d, UnsubCount: %d, ConnectCount: %+d, BreakCount: %+d", mdp.subCount, mdp.unsubCount, mdp.connectCount, mdp.breakCount)
			}
		}
	}()
}

func (mdp *MultiRedisMessageDispatcherPool) dealSubscribeRequest() {
	for {
		select {
		case <-mdp.ctx.Done():
			return
		case channel, ok := <-SubChannelChan:
			if ok {
				mdp.subCount++
				if _, ok := mdp.subChannels[channel]; !ok {
					mdp.subChannels[channel] = 1
					sub := mdp.redisClient.Subscribe()
					mdp.redisSubMap[channel] = sub
					mdp.subChannelsLen++
					go mdp.receiveAndPushByChannel(sub, channel)
				} else {
					mdp.subChannels[channel]++
				}
			}
		case channel, ok := <-UnsubChannelChan:
			if ok {
				mdp.unsubCount++
				if _, ok := mdp.subChannels[channel]; ok {
					mdp.subChannels[channel]--
					if mdp.subChannels[channel] <= 0 {
						delete(mdp.subChannels, channel)
						mdp.subChannelsLen--
						if sub, ok := mdp.redisSubMap[channel]; ok {
							if err := sub.Close(); err != nil {
								log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:dealSubscribeRequest close redis sub err: %s", err)
							}
						}
					}
				}
			}
		}
	}
}

func (mdp *MultiRedisMessageDispatcherPool) receiveAndPushByChannel(sub *redis.PubSub, channel string) {
	_ = sub.Subscribe(channel)
	ch := sub.Channel()
	for {
		select {
		case <-mdp.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			for _, redisChan := range mdp.dispatcherList {
				redisChan.pub(msg)
			}
		}
	}
}

func (mdp *MultiRedisMessageDispatcherPool) AddDispatcher(dispatcher *MultiRedisMessageDispatcher) {
	mdp.addDispatcherChan <- dispatcher
}

func (mdp *MultiRedisMessageDispatcherPool) DelDispatcher(dispatcher *MultiRedisMessageDispatcher) {
	mdp.delDispatcherChan <- dispatcher
}

func (mdp *MultiRedisMessageDispatcherPool) dealDispatcherRequest() {
	for {
		select {
		case <-mdp.ctx.Done():
			close(mdp.delDispatcherChan)
			close(mdp.addDispatcherChan)
			return
		case dispatcher, ok := <-mdp.addDispatcherChan:
			if ok {
				mdp.connectCount++
				mdp.dispatcherList = append(mdp.dispatcherList, dispatcher)
			}
		case dispatcher, ok := <-mdp.delDispatcherChan:
			if ok {
				mdp.breakCount++
				for key, dp := range mdp.dispatcherList {
					if dp == dispatcher {
						mdp.dispatcherList = append(mdp.dispatcherList[0:key], mdp.dispatcherList[key+1:]...)
						dispatcher.Close()
						break
					}
				}
			}
		}
	}
}
