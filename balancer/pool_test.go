package balancer

import (
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("pool", func() {
	var subject pool

	var addrsOf = func(p pool) []string {
		addrs := make([]string, len(p))
		for i, b := range p {
			addrs[i] = b.opt.Addr
		}
		return addrs
	}

	BeforeEach(func() {
		rand.Seed(100)
		subject = pool{
			&redisBackend{opt: mockOpts("host-1:6379"), up: 0, connections: 4, latency: int64(time.Millisecond)},
			&redisBackend{opt: mockOpts("host-2:6379"), up: 1, connections: 12, latency: int64(2 * time.Millisecond)},
			&redisBackend{opt: mockOpts("host-3:6379"), up: 1, connections: 8, latency: int64(3 * time.Millisecond)},
			&redisBackend{opt: mockOpts("host-4:6379"), up: 1, connections: 16, latency: int64(1 * time.Millisecond)},
		}
	})

	It("should select up", func() {
		Expect(addrsOf(subject.Up())).To(Equal([]string{
			"host-2:6379",
			"host-3:6379",
			"host-4:6379",
		}))
	})

	It("should select first up", func() {
		Expect(pool{}.FirstUp()).To(BeNil())
		Expect(subject.FirstUp().opt.Addr).To(Equal("host-2:6379"))
	})

	It("should select min up", func() {
		Expect(pool{}.MinUp(func(b *redisBackend) int64 { return 100 })).To(BeNil())
		Expect(subject.MinUp(func(b *redisBackend) int64 { return b.Connections() }).opt.Addr).To(Equal("host-3:6379"))
		Expect(subject.MinUp(func(b *redisBackend) int64 { return int64(b.Latency()) }).opt.Addr).To(Equal("host-4:6379"))
	})

	It("should select random", func() {
		res := make(map[string]int)
		for i := 0; i < 1000; i++ {
			res[subject.Random().opt.Addr]++
		}
		Expect(res).To(Equal(map[string]int{"host-1:6379": 259, "host-2:6379": 241, "host-3:6379": 265, "host-4:6379": 235}))
	})

	It("should select weighted-random", func() {
		Expect(pool{}.WeightedRandom(func(b *redisBackend) int64 { return 100 })).To(BeNil())

		res := make(map[string]int)
		for i := 0; i < 1000; i++ {
			res[subject.WeightedRandom(func(b *redisBackend) int64 { return b.Connections() }).opt.Addr]++
		}
		Expect(res).To(Equal(map[string]int{"host-1:6379": 418, "host-2:6379": 204, "host-3:6379": 302, "host-4:6379": 76}))
	})

	It("should select at position", func() {
		Expect(pool{}.At(0)).To(BeNil())
		Expect(subject.At(0).opt.Addr).To(Equal("host-1:6379"))
		Expect(subject.At(1).opt.Addr).To(Equal("host-2:6379"))
		Expect(subject.At(2).opt.Addr).To(Equal("host-3:6379"))
		Expect(subject.At(3).opt.Addr).To(Equal("host-4:6379"))
		Expect(subject.At(4).opt.Addr).To(Equal("host-1:6379"))
		Expect(subject.At(-1).opt.Addr).To(Equal("host-2:6379"))
		Expect(subject.At(-99).opt.Addr).To(Equal("host-4:6379"))
	})

})
