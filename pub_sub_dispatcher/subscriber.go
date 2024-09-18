package pub_sub_dispatcher

import (
	"bytes"
	"github.com/go-redis/redis/v8"
	"sync"
)

type Subscriber struct {
	Uuid             string
	mu               sync.RWMutex
	subscribeChannel map[string]struct{}
	publishChannel   chan *redis.Message
	redisDispatcher  *RedisDispatcher
}

func NewSubscriber(uuid string, redisDispatcher *RedisDispatcher) *Subscriber {
	return &Subscriber{
		Uuid:             uuid,
		mu:               sync.RWMutex{},
		subscribeChannel: make(map[string]struct{}),
		publishChannel:   make(chan *redis.Message, 100),
		redisDispatcher:  redisDispatcher,
	}
}

func (s *Subscriber) Channel() chan *redis.Message {
	return s.publishChannel
}

func (s *Subscriber) String() string {
	buf := bytes.Buffer{}
	s.mu.RLock()
	for channel := range s.subscribeChannel {
		buf.WriteString(channel)
		buf.WriteString(",")
	}
	s.mu.RUnlock()
	channels := buf.String()
	return channels[:len(channels)-1]
}

func (s *Subscriber) Channels() []string {
	channels := make([]string, 0)
	s.mu.RLock()
	for channel := range s.subscribeChannel {
		channels = append(channels, channel)
	}
	s.mu.RUnlock()
	return channels
}

func (s *Subscriber) ChannelsSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribeChannel)
}

func (s *Subscriber) Subscribe(channels ...string) {
	subscribeChannels := make([]string, 0)
	for _, channel := range channels {
		if _, ok := s.subscribeChannel[channel]; !ok {
			s.subscribeChannel[channel] = struct{}{}
			subscribeChannels = append(subscribeChannels, channel)
		}
	}
	s.redisDispatcher.Subscribe(s, subscribeChannels...)
}

func (s *Subscriber) Unsubscribe(channels ...string) {
	unSubscribeChannels := make([]string, 0)
	for _, channel := range channels {
		if _, ok := s.subscribeChannel[channel]; ok {
			delete(s.subscribeChannel, channel)
			unSubscribeChannels = append(unSubscribeChannels, channel)
		}
	}
	s.redisDispatcher.Unsubscribe(s, unSubscribeChannels...)
}

func (s *Subscriber) Close() {
	subscribeChannels := make([]string, 0)
	for channel := range s.subscribeChannel {
		subscribeChannels = append(subscribeChannels, channel)
	}
	s.redisDispatcher.Unsubscribe(s, subscribeChannels...)
}

func (s *Subscriber) closeChannel() {
	close(s.publishChannel)
}

func (s *Subscriber) Publish(msg *redis.Message) {
	s.publishChannel <- msg
}
