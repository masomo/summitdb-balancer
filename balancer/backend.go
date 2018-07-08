package balancer

import (
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

// Redis backend
type redisBackend struct {
	client *redis.Pool
	opt    *Options

	up, successes, failures, leader int32
	connections, latency            int64

	closer tomb.Tomb
}

// Backend structure
type Backend struct {
	Addr        string
	Connections int64
	Latency     time.Duration
	Status      bool
	Pool        *redis.Pool
}

func newRedisBackend(opt *Options) *redisBackend {
	backend := &redisBackend{
		client: &redis.Pool{
			MaxIdle:     opt.MaxIdle,
			IdleTimeout: time.Minute,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial(opt.Network, opt.Addr)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < time.Minute {
					return nil
				}
				_, err := c.Do("PING")
				return err
			},
		},
		opt: opt,
		up:  1,

		connections: 1e6,
		latency:     int64(time.Minute),
	}
	backend.startLoop()

	return backend
}

// Up returns true if up
func (b *redisBackend) Up() bool { return atomic.LoadInt32(&b.up) > 0 }

// Down returns true if down
func (b *redisBackend) Down() bool { return !b.Up() }

// Connections returns the number of connections
func (b *redisBackend) Connections() int64 { return atomic.LoadInt64(&b.connections) }

// Latency returns the current latency
func (b *redisBackend) Latency() time.Duration { return time.Duration(atomic.LoadInt64(&b.latency)) }

// Leader returns the current leader
func (b *redisBackend) Leader() bool { return atomic.LoadInt32(&b.leader) > 0 }

// Addr returns addr
func (b *redisBackend) Addr() string { return b.opt.Addr }

// Close shuts down the backend
func (b *redisBackend) Close() error {
	b.closer.Kill(nil)
	return b.closer.Wait()
}

func (b *redisBackend) checkBackend() {
	start := time.Now()

	conn := b.client.Get()
	defer conn.Close()

	reply, err := conn.Do("RAFTLEADER")
	if err != nil {
		b.updateStatus(false)
		atomic.CompareAndSwapInt32(&b.leader, 1, 0)
		return
	}

	atomic.StoreInt64(&b.latency, int64(time.Now().Sub(start)))
	atomic.StoreInt64(&b.connections, int64(b.client.ActiveCount()))

	b.updateStatus(true)

	if host, ok := reply.([]byte); ok {
		if string(host) == b.opt.Addr {
			atomic.CompareAndSwapInt32(&b.leader, 0, 1)
			return
		}
	}

	atomic.CompareAndSwapInt32(&b.leader, 1, 0)
}

func (b *redisBackend) incConnections(n int64) {
	atomic.AddInt64(&b.connections, n)
}

func (b *redisBackend) updateStatus(success bool) {
	if success {
		atomic.StoreInt32(&b.failures, 0)
		rise := b.opt.getRise()

		if n := int(atomic.AddInt32(&b.successes, 1)); n > rise {
			atomic.AddInt32(&b.successes, -1)
		} else if n == rise {
			atomic.CompareAndSwapInt32(&b.up, 0, 1)
		}
	} else {
		atomic.StoreInt32(&b.successes, 0)
		fall := b.opt.getFall()

		if n := int(atomic.AddInt32(&b.failures, 1)); n > fall {
			atomic.AddInt32(&b.failures, -1)
		} else if n == fall {
			atomic.CompareAndSwapInt32(&b.up, 1, 0)
		}
	}
}

func (b *redisBackend) startLoop() {
	interval := b.opt.getCheckInterval()
	b.checkBackend()

	b.closer.Go(func() error {
		for {
			select {
			case <-b.closer.Dying():
				return b.client.Close()
			case <-time.After(interval):
				b.checkBackend()
			}
		}
	})
}
