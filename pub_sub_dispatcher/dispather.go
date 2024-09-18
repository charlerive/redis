package pub_sub_dispatcher

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strings"
)

type DispatcherRequestType int

const (
	DispatcherRequestTypeSubscribe   = 1
	DispatcherRequestTypeUnsubscribe = 2
)

type dispatcherRequest struct {
	requestType DispatcherRequestType
	channel     []string
	subscriber  *Subscriber
}

type RedisDispatcher struct {
	Ctx            context.Context
	Cancel         context.CancelFunc
	redisClient    *redis.Client
	dispatcherMap  map[string]*dispatcher
	requestChannel chan dispatcherRequest
	printCounter   bool
}

func NewRedisDispatcher(ctx context.Context, redisClient *redis.Client) *RedisDispatcher {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	redisDispatcher := &RedisDispatcher{
		Ctx:            ctx,
		Cancel:         cancel,
		redisClient:    redisClient,
		dispatcherMap:  make(map[string]*dispatcher),
		requestChannel: make(chan dispatcherRequest, 2000),
	}
	go redisDispatcher.dealRequest()
	return redisDispatcher
}

func (rd *RedisDispatcher) SetPrintCounter(print bool) {
	rd.printCounter = print
}

func (rd *RedisDispatcher) Subscribe(subscriber *Subscriber, channel ...string) {
	rd.requestChannel <- dispatcherRequest{
		requestType: DispatcherRequestTypeSubscribe,
		channel:     channel,
		subscriber:  subscriber,
	}
}

func (rd *RedisDispatcher) Unsubscribe(subscriber *Subscriber, channel ...string) {
	rd.requestChannel <- dispatcherRequest{
		requestType: DispatcherRequestTypeUnsubscribe,
		channel:     channel,
		subscriber:  subscriber,
	}
}

func (rd *RedisDispatcher) dealRequest() {
	for {
		select {
		case <-rd.Ctx.Done():
			return
		case request, ok := <-rd.requestChannel:
			if !ok {
				continue
			}
			if request.requestType == DispatcherRequestTypeSubscribe {
				for _, channel := range request.channel {
					dp, exist := rd.dispatcherMap[channel]
					if !exist {
						dp = newDispatcher(rd.Ctx, channel, rd.redisClient)
					}
					dp.AddSubscriber(request.subscriber)
					rd.dispatcherMap[channel] = dp
				}
			} else if request.requestType == DispatcherRequestTypeUnsubscribe {
				for _, channel := range request.channel {
					dp, exist := rd.dispatcherMap[channel]
					if exist {
						dp.DelSubscriber(request.subscriber)
					}

				}
			}
		}
	}
}

type SubscriberRequestType int

const (
	SubscriberRequestTypeAdd = 1
	SubscriberRequestTypeDel = 2
)

type subscriberRequest struct {
	requestType SubscriberRequestType
	subscriber  *Subscriber
}

type dispatcher struct {
	ctx            context.Context
	cancel         context.CancelFunc
	channel        string
	pubSub         *redis.PubSub
	subscriber     map[string]*Subscriber
	requestChannel chan subscriberRequest
}

func newDispatcher(ctx context.Context, channel string, redisClient *redis.Client) *dispatcher {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	d := &dispatcher{
		ctx:            ctx,
		cancel:         cancel,
		channel:        channel,
		subscriber:     make(map[string]*Subscriber),
		requestChannel: make(chan subscriberRequest, 2000),
	}
	if strings.Contains(channel, "*") || strings.Contains(channel, "?") {
		d.pubSub = redisClient.PSubscribe(ctx, channel)
	} else {
		d.pubSub = redisClient.Subscribe(ctx, channel)
	}
	go d.dealRequest()
	return d
}

func (d *dispatcher) AddSubscriber(subscriber *Subscriber) {
	d.requestChannel <- subscriberRequest{
		requestType: SubscriberRequestTypeAdd,
		subscriber:  subscriber,
	}
}

func (d *dispatcher) DelSubscriber(subscriber *Subscriber) {
	d.requestChannel <- subscriberRequest{
		requestType: SubscriberRequestTypeDel,
		subscriber:  subscriber,
	}
}

func (d *dispatcher) dealRequest() {
	channel := d.pubSub.Channel()
	for {
		select {
		case <-d.ctx.Done():
			close(d.requestChannel)
			_ = d.pubSub.Close()
			return
		case request, ok := <-d.requestChannel:
			if !ok {
				continue
			}
			if request.requestType == SubscriberRequestTypeAdd {
				d.subscriber[request.subscriber.Uuid] = request.subscriber
			} else if request.requestType == SubscriberRequestTypeDel {
				delete(request.subscriber.subscribeChannel, d.channel)
				if len(request.subscriber.subscribeChannel) == 0 {
					request.subscriber.closeChannel()
				}
				delete(d.subscriber, request.subscriber.Uuid)
				if len(d.subscriber) == 0 {
					d.cancel()
				}
			}
		case msg, ok := <-channel:
			if !ok {
				continue
			}
			for _, subscriber := range d.subscriber {
				subscriber.Publish(msg)
			}
		}
	}
}
