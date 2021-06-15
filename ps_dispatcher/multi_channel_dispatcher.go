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
	subChannels map[string]struct{}
}

func (md *MultiRedisMessageDispatcher) Init() {
	md.pubChannel = make(chan *redis.Message, 100)
	md.subChannels = make(map[string]struct{})
}

func (md *MultiRedisMessageDispatcher) Channel() chan *redis.Message {
	return md.pubChannel
}

func (md *MultiRedisMessageDispatcher) String() string {
	buf := bytes.Buffer{}
	md.mu.RLock()
	for channel := range md.subChannels {
		buf.WriteString(channel)
		buf.WriteString(",")
	}
	md.mu.RUnlock()
	channels := buf.String()
	return channels[:len(channels)-1]
}

func (md *MultiRedisMessageDispatcher) Subscribe(channels ...string) {
	md.mu.Lock()
	for k, channel := range channels {
		if _, ok := md.subChannels[channel]; ok {
			channels = append(channels[0:k], channels[k+1:]...)
			continue
		}
		md.subChannels[channel] = struct{}{}
	}
	md.mu.Unlock()

	for _, s := range channels {
		SubChannelChan <- s
	}
}

func (md *MultiRedisMessageDispatcher) Unsubscribe(channels ...string) {
	md.mu.Lock()
	for k, channel := range channels {
		if _, ok := md.subChannels[channel]; !ok {
			channels = append(channels[0:k], channels[k+1:]...)
			continue
		}
		delete(md.subChannels, channel)
	}
	md.mu.Unlock()

	for _, s := range channels {
		UnsubChannelChan <- s
	}
}

func (md *MultiRedisMessageDispatcher) Close() {
	md.mu.Lock()
	defer md.mu.Unlock()
	for s := range md.subChannels {
		UnsubChannelChan <- s
		delete(md.subChannels, s)
	}
	close(md.pubChannel)
}

func (md *MultiRedisMessageDispatcher) pub(channel string, msg *redis.Message) {
	md.mu.RLock()
	defer md.mu.RUnlock()
	if _, ok := md.subChannels[channel]; ok {
		if len(md.pubChannel) < 90 {
			md.pubChannel <- msg
		}
	}
}

var (
	SubCount     = 0
	UnsubCount   = 0
	ConnectCount = 0
	BreakCount   = 0
)

type MultiRedisMessageDispatcherPool struct {
	ctx               context.Context
	redisClient       *redis.Client
	subChannels       map[string]int64
	redisSubMap       map[string]*redis.PubSub
	addDispatcherChan chan *MultiRedisMessageDispatcher
	delDispatcherChan chan *MultiRedisMessageDispatcher
	dispatcherList    []*MultiRedisMessageDispatcher
	mu                sync.Mutex
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
				mdp.mu.Lock()
				log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:PrintLength subChannelsLen: %d, redisChanListLen: %d", len(mdp.subChannels), len(mdp.dispatcherList))
				log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:PrintLength SubCount: %d, UnsubCount: %d, ConnectCount: %+d, BreakCount: %+d", SubCount, UnsubCount, ConnectCount, BreakCount)
				mdp.mu.Unlock()
			}
		}
	}()
}

func (mdp *MultiRedisMessageDispatcherPool) dealSubscribeRequest() {
	for {
		select {
		case <-mdp.ctx.Done():
			return
		case channel := <-SubChannelChan:
			go func() {
				if channel != "" {
					SubCount++
					mdp.mu.Lock()
					if _, ok := mdp.subChannels[channel]; !ok {
						mdp.subChannels[channel] = 1
						sub := mdp.redisClient.Subscribe()
						mdp.redisSubMap[channel] = sub
						go mdp.receiveAndPushByChannel(sub, channel)
					} else {
						mdp.subChannels[channel]++
					}
					mdp.mu.Unlock()
				}
			}()
		case channel := <-UnsubChannelChan:
			go func() {
				if channel != "" {
					UnsubCount++
					mdp.mu.Lock()
					if _, ok := mdp.subChannels[channel]; ok {
						mdp.subChannels[channel]--
						if mdp.subChannels[channel] <= 0 {
							delete(mdp.subChannels, channel)
							if sub, ok := mdp.redisSubMap[channel]; ok {
								if err := sub.Close(); err != nil {
									log.Printf("pubsubDispatcher:MultiRedisMessageDispatcherPool:dealSubscribeRequest close redis sub err: %s", err)
								}
							}
						}
					}
					mdp.mu.Unlock()
				}
			}()
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
				redisChan.pub(msg.Channel, msg)
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
				ConnectCount++
				mdp.mu.Lock()
				mdp.dispatcherList = append(mdp.dispatcherList, dispatcher)
				mdp.mu.Unlock()
			}
		case dispatcher, ok := <-mdp.delDispatcherChan:
			if ok {
				BreakCount++
				for key, dp := range mdp.dispatcherList {
					if dp == dispatcher {
						mdp.mu.Lock()
						mdp.dispatcherList = append(mdp.dispatcherList[0:key], mdp.dispatcherList[key+1:]...)
						mdp.mu.Unlock()
						dispatcher.Close()
						break
					}
				}
			}
		}
	}
}
