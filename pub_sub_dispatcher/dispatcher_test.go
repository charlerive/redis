package pub_sub_dispatcher

import (
	"context"
	"flag"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func TestRegisterRedisDispatcher(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		Password:    "123456",
		DB:          0,
		DialTimeout: time.Second * 5,
	})
	rd, err := RegisterRedisDispatcher(context.Background(), "ss", redisCli)
	if err != nil {
		t.Errorf("register redis dispatcher error: %s", err.Error())
		return
	}

	rd1, err1 := GetRedisDispatcher("ss")
	if err1 != nil {
		t.Errorf("get redis dispatcher error: %s", err1.Error())
		return
	}

	if rd != rd1 {
		t.Errorf("test register redis dispatcher error: %p, %p", rd1, rd)
	}
	t.Logf("test register redis dispatcher success: %p, %p", rd1, rd)
}

func TestRedisDispatcher(t *testing.T) {
	var cpuProfile = flag.String("cpuprofile", "redis_dispatcher_pprof.prof", "write cpu profile to file")
	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	redisCli := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		Password:    "123456",
		DB:          0,
		DialTimeout: time.Second * 5,
	})

	t.Logf("init redis client")

	wg := sync.WaitGroup{}
	rd := NewRedisDispatcher(context.Background(), redisCli)
	rd.SetPrintDuration(time.Second)
	rd.SetPrintCounter(true)

	sub1 := NewSubscriber("123", rd)
	sub1.Subscribe([]string{"test_channel:1", "test_channel:1"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-sub1.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("sub1 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	sub2 := NewSubscriber("234", rd)
	sub2.Subscribe([]string{"test_channel:2", "test_channel:3"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-sub2.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("sub2 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	sub3 := NewSubscriber("345", rd)
	sub3.Subscribe([]string{"test_channel:3", "test_channel:4", "test_channel:5"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-sub3.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("sub3 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	sub4 := NewSubscriber("456", rd)
	sub4.Subscribe([]string{"test_channel:2", "test_channel:3", "test_channel:4", "test_channel:5"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-sub4.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("sub4 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	sub5 := NewSubscriber("567", rd)
	sub5.Subscribe([]string{"test_channel:1", "test_channel:2", "test_channel:3", "test_channel:4", "test_channel:5"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-sub5.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("sub5 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	t.Logf("create subscriber")

	sub1.Unsubscribe("test_channel:1")
	sub1.Subscribe("test_channel:1*")

	time.Sleep(time.Second)
	go func() {
		for i := 0; i < 10000; i++ {
			pipe := redisCli.Pipeline()
			pipe.Publish(context.Background(), "test_channel:1", 1)
			pipe.Publish(context.Background(), "test_channel:2", 2)
			pipe.Publish(context.Background(), "test_channel:3", 3)
			pipe.Publish(context.Background(), "test_channel:4", 4)
			pipe.Publish(context.Background(), "test_channel:5", 5)
			pipe.Publish(context.Background(), "test_channel:6", 6)
			pipe.Publish(context.Background(), "test_channel:12", 12)
			_, _ = pipe.Exec(context.Background())
			time.Sleep(time.Millisecond * 10)
		}
	}()

	t.Logf("publish msg")

	sub1.Unsubscribe("test_channel:1")
	sub2.Unsubscribe("test_channel:2")
	sub3.Unsubscribe("test_channel:3")
	sub4.Unsubscribe("test_channel:4")
	sub5.Unsubscribe("test_channel:5")

	time.Sleep(time.Second * 50)

	sub1.Close()
	sub2.Close()
	sub3.Close()
	sub4.Close()
	sub5.Close()

	time.Sleep(time.Second * 5)

	t.Logf("close subscriber")

	wg.Wait()

	t.Logf("wg.wait")
}
