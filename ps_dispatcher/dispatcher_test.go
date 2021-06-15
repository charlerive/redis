package ps_dispatcher

import (
	"context"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func TestSingleRedisMessageDispatcher(t *testing.T) {
	var cpuProfile = flag.String("cpuprofile", "single_pprof.prof", "write cpu profile to file")
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
		Password:    "",
		DB:          0,
		DialTimeout: time.Second * 5,
	})
	// register dispatcher pool
	dp, err := RegisterSingleRedisDispatcherPool("default", redisCli, "test_channel:*")
	if err != nil && dp == nil {
		t.Fatalf("RegisterSingleRedisDispatcherPool err: %s", err)
	}
	// get dispatcher pool
	dp, err = GetSingleRedisDispatcherPool("default")
	if err != nil {
		t.Fatalf("GetSingleRedisDispatcherPool err: %s", err)
	}

	wg := sync.WaitGroup{}

	p1 := &SingleRedisMessageDispatcher{}
	p1.Init("test_channel:1")
	dp.AddDispatcher(p1)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-p1.Channel():
				if ok {
					msgCount++
					//t.Logf("p1 receive: %s", msg)
				} else {
					t.Logf("p1 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	p2 := &SingleRedisMessageDispatcher{}
	p2.Init("test_channel:2")
	dp.AddDispatcher(p2)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-p2.Channel():
				if ok {
					msgCount++
					//t.Logf("p2 receive: %s", msg)
				} else {
					t.Logf("p2 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	p3 := &SingleRedisMessageDispatcher{}
	p3.Init("test_channel:3")
	dp.AddDispatcher(p3)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-p3.Channel():
				if ok {
					msgCount++
					//t.Logf("p3 receive: %s", msg)
				} else {
					t.Logf("p3 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	p4 := &SingleRedisMessageDispatcher{}
	p4.Init("test_channel:4")
	dp.AddDispatcher(p4)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-p4.Channel():
				if ok {
					msgCount++
					//t.Logf("p4 receive: %s", msg)
				} else {
					t.Logf("p4 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	pipe := redisCli.Pipeline()
	for i := 0; i < 30000; i++ {
		pipe.Publish("test_channel:1", 1)
		pipe.Publish("test_channel:2", 2)
		pipe.Publish("test_channel:3", 3)
		pipe.Publish("test_channel:4", 4)
	}
	_, _ = pipe.Exec()

	time.Sleep(time.Millisecond * 60)
	dp.DelDispatcher(p1)
	dp.DelDispatcher(p2)
	dp.DelDispatcher(p3)
	dp.DelDispatcher(p4)
	wg.Wait()
}

func TestMultiRedisMessageDispatcher(t *testing.T) {
	var cpuProfile = flag.String("cpuprofile", "multi_pprof.prof", "write cpu profile to file")
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
		Password:    "",
		DB:          0,
		DialTimeout: time.Second * 5,
	})

	wg := sync.WaitGroup{}

	mdp := NewMultiRedisMessageDispatcherPool(context.Background(), redisCli)
	//mdp.PrintLength(time.Millisecond * 100)

	md1 := &MultiRedisMessageDispatcher{}
	md1.Init()
	mdp.AddDispatcher(md1)
	md1.Subscribe([]string{"test_channel:1", "test_channel:2", "test_channel:3", "test_channel:4", "test_channel:5", "test_channel:6"}...)
	//log.Printf("channel: %s", md1.String())
	//md1.Subscribe([]string{"test_channel:1", "test_channel:2"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-md1.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("md1 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	md2 := &MultiRedisMessageDispatcher{}
	md2.Init()
	mdp.AddDispatcher(md2)
	md2.Subscribe([]string{"test_channel:2", "test_channel:3"}...)
	//md2.Subscribe([]string{"test_channel:1", "test_channel:2"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-md2.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("md2 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	md3 := &MultiRedisMessageDispatcher{}
	md3.Init()
	mdp.AddDispatcher(md3)
	md3.Subscribe([]string{"test_channel:3", "test_channel:4"}...)
	//md3.Subscribe([]string{"test_channel:1", "test_channel:2"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-md3.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("md3 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	md4 := &MultiRedisMessageDispatcher{}
	md4.Init()
	mdp.AddDispatcher(md4)
	md4.Subscribe([]string{"test_channel:4", "test_channel:5"}...)
	//md4.Subscribe([]string{"test_channel:1", "test_channel:2"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-md4.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("md4 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	md5 := &MultiRedisMessageDispatcher{}
	md5.Init()
	mdp.AddDispatcher(md5)
	md5.Subscribe([]string{"test_channel:4", "test_channel:5", "test_channel:6"}...)
	//md5.Subscribe([]string{"test_channel:1", "test_channel:2"}...)
	go func() {
		wg.Add(1)
		msgCount := 0
		for {
			select {
			case _, ok := <-md5.Channel():
				if ok {
					msgCount++
				} else {
					t.Logf("md5 receive msgCount: %d", msgCount)
					wg.Done()
					return
				}
			}
		}
	}()

	md1.Unsubscribe("test_channel:1")
	log.Printf("md1.channels: %s", md1.String())

	// sleep for MultiRedisMessageDispatcher add
	time.Sleep(time.Second)

	for i := 0; i < 10000; i++ {
		pipe := redisCli.Pipeline()
		pipe.Publish("test_channel:1", 1)
		pipe.Publish("test_channel:2", 2)
		pipe.Publish("test_channel:3", 3)
		pipe.Publish("test_channel:4", 4)
		pipe.Publish("test_channel:5", 5)
		pipe.Publish("test_channel:6", 6)
		_, _ = pipe.Exec()
	}

	// wait for msg receive
	time.Sleep(time.Second)
	mdp.DelDispatcher(md1)
	mdp.DelDispatcher(md2)
	mdp.DelDispatcher(md3)
	mdp.DelDispatcher(md4)
	mdp.DelDispatcher(md5)
	wg.Wait()
}
