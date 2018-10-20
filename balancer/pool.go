package balancer

import (
	"math"
	"math/rand"
)

type pool []*redisBackend

// Up returns all backends that are up
func (p pool) Up() pool {
	return p.all(func(b *redisBackend) bool { return b.Up() })
}

// FirstUp returns the first backend that is up
func (p pool) FirstUp() *redisBackend {
	return p.first(func(b *redisBackend) bool { return b.Up() })
}

// Leader returns the leader backend
func (p pool) Leader() *redisBackend {
	return p.first(func(b *redisBackend) bool { return b.Leader() })
}

// MinUp returns the backend with the minumum result that is up
func (p pool) MinUp(minimum func(*redisBackend) int64) *redisBackend {
	min := int64(math.MaxInt64)
	pos := -1
	for n, b := range p {
		if b.Up() {
			if num := minimum(b); num < min {
				pos, min = n, num
			}
		}
	}

	if pos < 0 {
		return nil
	}
	return p[pos]
}

// Random returns a random backend
func (p pool) Random() *redisBackend {
	if size := len(p); size > 0 {
		return p[rand.Intn(size)]
	}
	return nil
}

// At picks a pool item using at pos (seed)
func (p pool) At(pos int) *redisBackend {
	n := len(p)
	if n < 1 {
		return nil
	}
	if pos %= n; pos < 0 {
		pos *= -1
	}
	return p[pos]
}

// WeightedRandom returns a weighted-random backend
func (p pool) WeightedRandom(weight func(*redisBackend) int64) *redisBackend {
	if len(p) < 1 {
		return nil
	}

	var min, max int64 = math.MaxInt64, 1
	weights := make([]int64, len(p))
	for n, b := range p {
		w := weight(b)
		if w > max {
			max = w
		}
		if w < min {
			min = w
		}
		weights[n] = w
	}

	var sum int64
	for n, w := range weights {
		w = min + max - w
		sum = sum + w
		weights[n] = w
	}

	if sum <= 0 {
		sum = 1
	}

	mark := rand.Int63n(sum)
	for n, w := range weights {
		if mark -= w; mark <= 0 {
			return p[n]
		}
	}

	// We should never reach this point if the slice wasn't empty
	return nil
}

// selects all backends given a criteria
func (p pool) all(criteria func(*redisBackend) bool) pool {
	res := make(pool, 0, len(p))
	for _, b := range p {
		if criteria(b) {
			res = append(res, b)
		}
	}
	return res
}

// returns the first matching backend given a criteria, or nil when nothing matches
func (p pool) first(criteria func(*redisBackend) bool) *redisBackend {
	for _, b := range p {
		if criteria(b) {
			return b
		}
	}
	return nil
}
