package ps_dispatcher

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"sync"
)

var (
	singleDispatcherMap = make(map[string]*SingleRedisMessageDispatcherPool)
	singleDispatcherMu  = sync.RWMutex{}
)

func GetSingleRedisDispatcherPool(alias string) (*SingleRedisMessageDispatcherPool, error) {
	singleDispatcherMu.RLock()
	defer singleDispatcherMu.RUnlock()
	if dispatch, ok := singleDispatcherMap[alias]; ok {
		return dispatch, nil
	}
	return nil, fmt.Errorf("pubsubDispatcher:GetSingleRedisDispatcherPool dispatcher not fund")
}

func RegisterSingleRedisDispatcherPool(alias string, redisCli *redis.Client, subChannel string) (dp *SingleRedisMessageDispatcherPool, err error) {
	singleDispatcherMu.Lock()
	ok := false
	if dp, ok = singleDispatcherMap[alias]; ok {
		singleDispatcherMu.Unlock()
		return dp, fmt.Errorf("pubsubDispatcher:RegisterSingleRedisDispatcherPool fail. alias: %s has allready used. ", alias)
	}
	singleDispatcherMap[alias] = &SingleRedisMessageDispatcherPool{}
	singleDispatcherMu.Unlock()

	defer func() {
		singleDispatcherMu.Lock()
		if err != nil || singleDispatcherMap[alias].ctx == nil {
			delete(singleDispatcherMap, alias)
		}
		singleDispatcherMu.Unlock()
	}()
	subscribe := redisCli.Subscribe()
	err = subscribe.PSubscribe(subChannel)
	if err != nil {
		log.Printf("pubsubDispatcher:RegisterSingleRedisDispatcherPool subscribe.PSubscribe %s fail, err: %s", subChannel, err.Error())
		return nil, fmt.Errorf("Dispatcher:RegisterSingleRedisDispatcherPool subscribe.PSubscribe %s fail, err: %s", subChannel, err.Error())
	}
	var iFace interface{}
	iFace, err = subscribe.Receive()
	if err != nil {
		log.Printf("pubsubDispatcher:RegisterSingleRedisDispatcherPool subscribe.Receive fail. err:%s ", err.Error())
		return nil, fmt.Errorf("Dispatcher:RegisterSingleRedisDispatcherPool subscribe.Receive fail. err:%s ", err.Error())
	}
	switch iFace.(type) {
	case *redis.Subscription:
		// subscribe succeeded
		log.Printf("pubsubDispatcher:RegisterSingleRedisDispatcherPool subscribe success, channel: %s", subChannel)
	case *redis.Message:
		// received first message
	case *redis.Pong:
		// pong received
	default:
		// handle error
		log.Printf("pubsubDispatcher:RegisterSingleRedisDispatcherPool subscribe fail, channel: %s", subChannel)
		return nil, fmt.Errorf("Dispatcher:RegisterSingleRedisDispatcherPool subscribe fail, channel: %s", subChannel)
	}
	ch := subscribe.Channel()

	dp = NewRedisMessageDispatcherPool(context.Background(), subscribe, ch)
	singleDispatcherMu.Lock()
	singleDispatcherMap[alias] = dp
	singleDispatcherMu.Unlock()
	return dp, nil
}

// redis转发器-单通道
type SingleRedisMessageDispatcher struct {
	isClose    bool
	subChannel string
	pubChannel chan *redis.Message
}

func (rd *SingleRedisMessageDispatcher) Init(subChannel string) {
	rd.subChannel = subChannel
	rd.pubChannel = make(chan *redis.Message, 100)
}

func (rd *SingleRedisMessageDispatcher) Channel() chan *redis.Message {
	return rd.pubChannel
}

func (rd *SingleRedisMessageDispatcher) String() string {
	return rd.subChannel
}

func (rd *SingleRedisMessageDispatcher) Close() {
	rd.isClose = true
	close(rd.pubChannel)
}

func (rd *SingleRedisMessageDispatcher) pub(msg *redis.Message) {
	if !rd.isClose && len(rd.pubChannel) < 90 {
		rd.pubChannel <- msg
	}
}

func NewRedisMessageDispatcherPool(ctx context.Context, subscribe *redis.PubSub, redisChan <-chan *redis.Message) *SingleRedisMessageDispatcherPool {
	if ctx == nil {
		ctx = context.Background()
	}
	pool := &SingleRedisMessageDispatcherPool{
		ctx:       ctx,
		subscribe: subscribe,
		redisChan: redisChan,
	}
	pool.dispatcherMap = make(map[string][]*SingleRedisMessageDispatcher)
	pool.pubChannel = make(chan *redis.Message, 80)
	pool.addDispatcherChan = make(chan *SingleRedisMessageDispatcher, 500)
	pool.delDispatcherChan = make(chan *SingleRedisMessageDispatcher, 500)

	go pool.dealDispatcherRequest()
	go pool.receive()

	return pool
}

// redis转发器池-单通道
type SingleRedisMessageDispatcherPool struct {
	ctx               context.Context
	subscribe         *redis.PubSub
	redisChan         <-chan *redis.Message
	needPub           bool
	pubChannel        chan *redis.Message
	addDispatcherChan chan *SingleRedisMessageDispatcher
	delDispatcherChan chan *SingleRedisMessageDispatcher
	dispatcherMu      sync.RWMutex
	dispatcherMap     map[string][]*SingleRedisMessageDispatcher
}

func (p *SingleRedisMessageDispatcherPool) Channel() <-chan *redis.Message {
	p.needPub = true
	return p.pubChannel
}

func (p *SingleRedisMessageDispatcherPool) Subscribe() *redis.PubSub {
	return p.subscribe
}

func (p *SingleRedisMessageDispatcherPool) AddDispatcher(dispatcher *SingleRedisMessageDispatcher) {
	p.addDispatcherChan <- dispatcher
}

func (p *SingleRedisMessageDispatcherPool) DelDispatcher(dispatcher *SingleRedisMessageDispatcher) {
	p.delDispatcherChan <- dispatcher
}

func (p *SingleRedisMessageDispatcherPool) dealDispatcherRequest() {
	for {
		select {
		case <-p.ctx.Done():
			close(p.delDispatcherChan)
			close(p.addDispatcherChan)
			return
		case dispatcher := <-p.addDispatcherChan:
			if dispatcher == nil {
				continue
			}
			go func() {
				subChannel := dispatcher.subChannel
				p.dispatcherMu.Lock()
				_, ok := p.dispatcherMap[subChannel]
				if !ok {
					p.dispatcherMap[subChannel] = make([]*SingleRedisMessageDispatcher, 0)
				}
				p.dispatcherMap[subChannel] = append(p.dispatcherMap[subChannel], dispatcher)
				p.dispatcherMu.Unlock()
			}()
		case dispatcher := <-p.delDispatcherChan:
			if dispatcher == nil {
				continue
			}
			go func() {
				subChannel := dispatcher.subChannel
				p.dispatcherMu.Lock()
				if dispatcherList, ok := p.dispatcherMap[subChannel]; ok {
					for key, d := range dispatcherList {
						if d == dispatcher {
							p.dispatcherMap[subChannel] = append(p.dispatcherMap[subChannel][0:key], p.dispatcherMap[subChannel][key+1:]...)
						}
					}
					dispatcher.Close()
				}
				p.dispatcherMu.Unlock()
			}()
		}
	}
}

func (p *SingleRedisMessageDispatcherPool) receive() {
	for {
		select {
		case <-p.ctx.Done():
			close(p.pubChannel)
			return
		case msg := <-p.redisChan:
			if msg == nil {
				continue
			}
			p.pub(msg)
			// 分发到其他订阅组
			p.dispatcherMu.RLock()
			dispatcherList, ok := p.dispatcherMap[msg.Channel]
			p.dispatcherMu.RUnlock()
			if ok {
				for _, dispatcher := range dispatcherList {
					dispatcher.pub(msg)
				}
			}
		}
	}
}

func (p *SingleRedisMessageDispatcherPool) pub(msg *redis.Message) {
	if !p.needPub {
		return
	}
	if len(p.pubChannel) < 60 {
		p.pubChannel <- msg
	} else {
		log.Printf("pubsubDispatcher:SingleRedisMessageDispatcherPool:pub len(p.pubChannel) is more than 60, message drop. message: %+v", msg)
	}
}
