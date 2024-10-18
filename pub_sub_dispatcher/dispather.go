package pub_sub_dispatcher

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"strings"
	"time"
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
	printDuration  time.Duration
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

func (rd *RedisDispatcher) SetPrintDuration(duration time.Duration) {
	rd.printDuration = duration
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
	if rd.printDuration == 0 {
		rd.printDuration = time.Minute
	}
	timer := time.NewTimer(rd.printDuration)
	ticker := time.NewTicker(time.Second)
	dpTicker := time.NewTicker(time.Minute)
	duration := rd.printDuration
	subscriberCounter, unsubscribeCounter := int64(0), int64(0)
	for {
		select {
		case <-rd.Ctx.Done():
			timer.Stop()
			ticker.Stop()
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
						rd.dispatcherMap[channel] = dp
					}
					dp.AddSubscriber(request.subscriber)
					subscriberCounter++
				}
			} else if request.requestType == DispatcherRequestTypeUnsubscribe {
				for _, channel := range request.channel {
					dp, exist := rd.dispatcherMap[channel]
					if exist {
						dp.DelSubscriber(request.subscriber)
						unsubscribeCounter++
					}

				}
			}
		case <-dpTicker.C:
			for channel, dp := range rd.dispatcherMap {
				if dp.lastRequestTime.Add(time.Minute).Before(time.Now()) && len(dp.subscriberMap) == 0 {
					dp.cancel()
					delete(rd.dispatcherMap, channel)
				}
			}
		case <-ticker.C:
			if duration != rd.printDuration {
				timer.Reset(rd.printDuration)
				duration = rd.printDuration
			}
		case <-timer.C:
			if rd.printCounter {
				channelLen, subscriberLen, dispatcherCounter := len(rd.dispatcherMap), int64(0), uint64(0)
				subscriberIdMap := make(map[string]struct{})
				for _, dp := range rd.dispatcherMap {
					dispatcherCounter += dp.dispatcherCounter
					for _, subscriber := range dp.subscriberMap {
						if _, ok := subscriberIdMap[subscriber.Uuid]; !ok {
							subscriberLen++
							subscriberIdMap[subscriber.Uuid] = struct{}{}
						}
					}
				}
				log.Printf("RedisDispatcher print counter, channel len: %d, subscriber length: %d, subscribe counter: %d, unsubscribe counter: %d, dispatcher counter: %d",
					channelLen, subscriberLen, subscriberCounter, unsubscribeCounter, dispatcherCounter)
			}
			timer.Reset(rd.printDuration)
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
	ctx               context.Context
	cancel            context.CancelFunc
	channel           string
	dispatcherCounter uint64
	pubSub            *redis.PubSub
	subscriberMap     map[string]*Subscriber
	requestChannel    chan subscriberRequest
	lastRequestTime   time.Time
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
		subscriberMap:  make(map[string]*Subscriber),
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
	if d.ctx.Err() == nil {
		d.requestChannel <- subscriberRequest{
			requestType: SubscriberRequestTypeAdd,
			subscriber:  subscriber,
		}
	}
}

func (d *dispatcher) DelSubscriber(subscriber *Subscriber) {
	if d.ctx.Err() == nil {
		d.requestChannel <- subscriberRequest{
			requestType: SubscriberRequestTypeDel,
			subscriber:  subscriber,
		}
	}
}

func (d *dispatcher) dealRequest() {
	channel := d.pubSub.Channel()
	d.lastRequestTime = time.Now()
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
			d.lastRequestTime = time.Now()
			if request.requestType == SubscriberRequestTypeAdd {
				d.subscriberMap[request.subscriber.Uuid] = request.subscriber
			} else if request.requestType == SubscriberRequestTypeDel {
				delete(d.subscriberMap, request.subscriber.Uuid)
			}
		case msg, ok := <-channel:
			if !ok {
				continue
			}
			for _, subscriber := range d.subscriberMap {
				d.dispatcherCounter++
				subscriber.Publish(msg)
			}

		}
	}
}
