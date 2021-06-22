package ps_dispatcher

import (
	"bytes"
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	SubChannelChan   = make(chan string, 2000)
	UnsubChannelChan = make(chan string, 2000)
)

type MultiChannelDispatcher struct {
	mu             sync.RWMutex
	pubChannel     chan *redis.Message
	subChannels    sync.Map // map[string]struct{}
	subPatterns    sync.Map // map[string]struct{}
	subPatternsLen int
}

func (md *MultiChannelDispatcher) Init() {
	md.pubChannel = make(chan *redis.Message, 100)
	md.subChannels = sync.Map{}
	md.subPatterns = sync.Map{}
}

func (md *MultiChannelDispatcher) Channel() chan *redis.Message {
	return md.pubChannel
}

func (md *MultiChannelDispatcher) String() string {
	buf := bytes.Buffer{}
	md.subChannels.Range(func(channel, _ interface{}) bool {
		buf.WriteString(channel.(string))
		buf.WriteString(",")
		return true
	})
	md.subPatterns.Range(func(pattern, _ interface{}) bool {
		buf.WriteString(pattern.(string))
		buf.WriteString(",")
		return true
	})
	channels := buf.String()
	return channels[:len(channels)-1]
}

func (md *MultiChannelDispatcher) Subscribe(channels ...string) {
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

func (md *MultiChannelDispatcher) Unsubscribe(channels ...string) {
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

func (md *MultiChannelDispatcher) PSubscribe(patterns ...string) {
	for k, pattern := range patterns {
		if _, ok := md.subPatterns.Load(pattern); ok {
			patterns = append(patterns[0:k], patterns[k+1:]...)
			continue
		}
		md.subPatternsLen++
		md.subPatterns.Store(pattern, struct{}{})
	}

	for _, p := range patterns {
		SubChannelChan <- p
	}
}

func (md *MultiChannelDispatcher) PUnsubscribe(patterns ...string) {
	for k, pattern := range patterns {
		if _, ok := md.subPatterns.Load(pattern); !ok {
			patterns = append(patterns[0:k], patterns[k+1:]...)
			continue
		}
		md.subPatternsLen--
		md.subPatterns.Delete(pattern)
	}

	for _, p := range patterns {
		UnsubChannelChan <- p
	}
}

func (md *MultiChannelDispatcher) Close() {
	md.subChannels.Range(func(channel, _ interface{}) bool {
		UnsubChannelChan <- channel.(string)
		md.subChannels.Delete(channel)
		return true
	})

	md.subPatterns.Range(func(pattern, _ interface{}) bool {
		UnsubChannelChan <- pattern.(string)
		md.subPatterns.Delete(pattern)
		return true
	})
	close(md.pubChannel)
}

func (md *MultiChannelDispatcher) pub(msg *redis.Message) {
	if msg.Pattern == "" {
		if _, ok := md.subChannels.Load(msg.Channel); ok {
			if len(md.pubChannel) < 90 {
				md.pubChannel <- msg
			}
		}
	} else if md.subPatternsLen > 0 {
		if _, ok := md.subPatterns.Load(msg.Pattern); ok {
			if len(md.pubChannel) < 90 {
				md.pubChannel <- msg
			}
		}
	}
}

type MultiChannelDispatcherPool struct {
	ctx               context.Context
	redisClient       *redis.Client
	subChannels       map[string]int64
	redisSubMap       map[string]*redis.PubSub
	addDispatcherChan chan *MultiChannelDispatcher
	delDispatcherChan chan *MultiChannelDispatcher
	dispatcherList    []*MultiChannelDispatcher
	mu                sync.Mutex

	subChannelsLen int64
	subCount       int64
	unsubCount     int64
	connectCount   int64
	breakCount     int64
}

func NewMultiChannelDispatcherPool(ctx context.Context, redisClient *redis.Client) (mdp *MultiChannelDispatcherPool) {
	if ctx == nil {
		ctx = context.Background()
	}
	mdp = &MultiChannelDispatcherPool{
		ctx:               ctx,
		redisClient:       redisClient,
		subChannels:       make(map[string]int64),
		redisSubMap:       make(map[string]*redis.PubSub),
		addDispatcherChan: make(chan *MultiChannelDispatcher, 100),
		delDispatcherChan: make(chan *MultiChannelDispatcher, 100),
		dispatcherList:    make([]*MultiChannelDispatcher, 0),
		mu:                sync.Mutex{},
	}

	go mdp.dealSubscribeRequest()
	go mdp.dealDispatcherRequest()

	return mdp
}

func (mdp *MultiChannelDispatcherPool) PrintLength(duration time.Duration) {
	timer := time.NewTicker(duration)
	go func() {
		for {
			select {
			case <-mdp.ctx.Done():
				return
			case <-timer.C:
				log.Printf("pubsubDispatcher:MultiChannelDispatcherPool:PrintLength subChannelsLen: %d, redisChanListLen: %d", mdp.subChannelsLen, len(mdp.dispatcherList))
				log.Printf("pubsubDispatcher:MultiChannelDispatcherPool:PrintLength SubCount: %d, UnsubCount: %d, ConnectCount: %+d, BreakCount: %+d", mdp.subCount, mdp.unsubCount, mdp.connectCount, mdp.breakCount)
			}
		}
	}()
}

func (mdp *MultiChannelDispatcherPool) dealSubscribeRequest() {
	for {
		select {
		case <-mdp.ctx.Done():
			return
		case channel, ok := <-SubChannelChan:
			if ok {
				mdp.subCount++
				if _, ok := mdp.subChannels[channel]; !ok {
					mdp.subChannels[channel] = 1
					sub := mdp.redisClient.Subscribe(mdp.ctx)
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
								log.Printf("pubsubDispatcher:MultiChannelDispatcherPool:dealSubscribeRequest close redis sub err: %s", err)
							}
						}
					}
				}
			}
		}
	}
}

func (mdp *MultiChannelDispatcherPool) receiveAndPushByChannel(sub *redis.PubSub, channel string) {
	if strings.Contains(channel, "*") || strings.Contains(channel, "?") {
		_ = sub.PSubscribe(mdp.ctx, channel)
	} else {
		_ = sub.Subscribe(mdp.ctx, channel)
	}
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

func (mdp *MultiChannelDispatcherPool) AddDispatcher(dispatcher *MultiChannelDispatcher) {
	mdp.addDispatcherChan <- dispatcher
}

func (mdp *MultiChannelDispatcherPool) DelDispatcher(dispatcher *MultiChannelDispatcher) {
	mdp.delDispatcherChan <- dispatcher
}

func (mdp *MultiChannelDispatcherPool) dealDispatcherRequest() {
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
