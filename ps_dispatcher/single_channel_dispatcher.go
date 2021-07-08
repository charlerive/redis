package ps_dispatcher

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

var (
	singleDispatcherMap = sync.Map{} //make(map[string]*SingleChannelDispatcherPool)
)

func GetSingleChannelDispatcherPool(alias string) (*SingleChannelDispatcherPool, error) {
	if dispatchLoad, ok := singleDispatcherMap.Load(alias); ok && dispatchLoad.(*SingleChannelDispatcherPool).ctx != nil {
		return dispatchLoad.(*SingleChannelDispatcherPool), nil
	}
	return nil, fmt.Errorf("pubsubDispatcher:GetSingleChannelDispatcherPool dispatcher not fund")
}

func RegisterSingleChannelDispatcherPool(alias string, redisCli *redis.Client, subChannel string) (dp *SingleChannelDispatcherPool, err error) {
	if _, ok := singleDispatcherMap.Load(alias); ok {
		return nil, fmt.Errorf("pubsubDispatcher:RegisterSingleChannelDispatcherPool fail. alias: %s has allready used. ", alias)
	}
	singleDispatcherMap.Store(alias, &SingleChannelDispatcherPool{})
	defer func() {
		if err != nil {
			singleDispatcherMap.Delete(alias)
		}
		if dispatcherLoad, ok := singleDispatcherMap.Load(alias); !ok || dispatcherLoad.(*SingleChannelDispatcherPool).ctx == nil {
			singleDispatcherMap.Delete(alias)
		}
	}()

	subscribe := redisCli.Subscribe(context.Background())
	err = subscribe.PSubscribe(context.Background(), subChannel)
	if err != nil {
		log.Printf("pubsubDispatcher:RegisterSingleChannelDispatcherPool subscribe.PSubscribe %s fail, err: %s", subChannel, err.Error())
		return nil, fmt.Errorf("Dispatcher:RegisterSingleChannelDispatcherPool subscribe.PSubscribe %s fail, err: %s", subChannel, err.Error())
	}
	var iFace interface{}
	iFace, err = subscribe.Receive(context.Background())
	if err != nil {
		log.Printf("pubsubDispatcher:RegisterSingleChannelDispatcherPool subscribe.Receive fail. err:%s ", err.Error())
		return nil, fmt.Errorf("Dispatcher:RegisterSingleChannelDispatcherPool subscribe.Receive fail. err:%s ", err.Error())
	}
	switch iFace.(type) {
	case *redis.Subscription:
		// subscribe succeeded
		log.Printf("pubsubDispatcher:RegisterSingleChannelDispatcherPool subscribe success, channel: %s", subChannel)
	case *redis.Message:
		// received first message
	case *redis.Pong:
		// pong received
	default:
		// handle error
		log.Printf("pubsubDispatcher:RegisterSingleChannelDispatcherPool subscribe fail, channel: %s", subChannel)
		return nil, fmt.Errorf("Dispatcher:RegisterSingleChannelDispatcherPool subscribe fail, channel: %s", subChannel)
	}
	ch := subscribe.Channel()

	dp = NewSingleChannelDispatcherPool(context.Background(), subscribe, ch)
	singleDispatcherMap.Store(alias, dp)
	return dp, nil
}

// redis转发器-单通道
type SingleChannelDispatcher struct {
	isClose    bool
	subChannel string
	pubChannel chan *redis.Message
}

func (rd *SingleChannelDispatcher) Init(subChannel string) {
	rd.subChannel = subChannel
	rd.pubChannel = make(chan *redis.Message, 100)
}

func (rd *SingleChannelDispatcher) Channel() chan *redis.Message {
	return rd.pubChannel
}

func (rd *SingleChannelDispatcher) String() string {
	return rd.subChannel
}

func (rd *SingleChannelDispatcher) Close() {
	rd.isClose = true
	close(rd.pubChannel)
}

func (rd *SingleChannelDispatcher) pub(msg *redis.Message) {
	if !rd.isClose && len(rd.pubChannel) < 90 {
		rd.pubChannel <- msg
	}
}

func NewSingleChannelDispatcherPool(ctx context.Context, subscribe *redis.PubSub, redisChan <-chan *redis.Message) *SingleChannelDispatcherPool {
	if ctx == nil {
		ctx = context.Background()
	}
	pool := &SingleChannelDispatcherPool{
		ctx:       ctx,
		subscribe: subscribe,
		redisChan: redisChan,
	}
	pool.dispatcherMap = make(map[string][]*SingleChannelDispatcher)
	pool.pubChannel = make(chan *redis.Message, 80)
	pool.addDispatcherChan = make(chan *SingleChannelDispatcher, 500)
	pool.delDispatcherChan = make(chan *SingleChannelDispatcher, 500)

	go pool.dealDispatcherRequestAndReceive()

	return pool
}

// redis转发器池-单通道
type SingleChannelDispatcherPool struct {
	ctx               context.Context
	subscribe         *redis.PubSub
	redisChan         <-chan *redis.Message
	needPub           bool
	pubChannel        chan *redis.Message
	addDispatcherChan chan *SingleChannelDispatcher
	delDispatcherChan chan *SingleChannelDispatcher
	dispatcherMap     map[string][]*SingleChannelDispatcher
}

func (p *SingleChannelDispatcherPool) Channel() <-chan *redis.Message {
	p.needPub = true
	return p.pubChannel
}

func (p *SingleChannelDispatcherPool) Subscribe() *redis.PubSub {
	return p.subscribe
}

func (p *SingleChannelDispatcherPool) AddDispatcher(dispatcher *SingleChannelDispatcher) {
	p.addDispatcherChan <- dispatcher
}

func (p *SingleChannelDispatcherPool) DelDispatcher(dispatcher *SingleChannelDispatcher) {
	p.delDispatcherChan <- dispatcher
}

func (p *SingleChannelDispatcherPool) dealDispatcherRequestAndReceive() {
	for {
		select {
		case <-p.ctx.Done():
			close(p.delDispatcherChan)
			close(p.addDispatcherChan)
			close(p.pubChannel)
			return
		case dispatcher := <-p.addDispatcherChan:
			if dispatcher == nil {
				continue
			}
			subChannel := dispatcher.subChannel
			_, ok := p.dispatcherMap[subChannel]
			if !ok {
				p.dispatcherMap[subChannel] = make([]*SingleChannelDispatcher, 0)
			}
			p.dispatcherMap[subChannel] = append(p.dispatcherMap[subChannel], dispatcher)
		case dispatcher := <-p.delDispatcherChan:
			if dispatcher == nil {
				continue
			}
			subChannel := dispatcher.subChannel
			if dispatcherList, ok := p.dispatcherMap[subChannel]; ok {
				for key, d := range dispatcherList {
					if d == dispatcher {
						if key+1 < len(dispatcherList) {
							p.dispatcherMap[subChannel] = append(p.dispatcherMap[subChannel][0:key], p.dispatcherMap[subChannel][key+1:]...)
						} else {
							p.dispatcherMap[subChannel] = p.dispatcherMap[subChannel][0:key]
						}
					}
				}
				dispatcher.Close()
				if len(p.dispatcherMap[subChannel]) == 0 {
					delete(p.dispatcherMap, subChannel)
				}
			}
		case msg := <-p.redisChan:
			if msg == nil {
				continue
			}
			p.pub(msg)
			// 分发到其他订阅组
			dispatcherList, ok := p.dispatcherMap[msg.Channel]
			if ok {
				for _, dispatcher := range dispatcherList {
					dispatcher.pub(msg)
				}
			}
		}
	}
}

func (p *SingleChannelDispatcherPool) pub(msg *redis.Message) {
	if !p.needPub {
		return
	}
	if len(p.pubChannel) < 60 {
		p.pubChannel <- msg
	} else {
		log.Printf("pubsubDispatcher:SingleChannelDispatcherPool:pub len(p.pubChannel) is more than 60, message drop. message: %+v", msg)
	}
}
