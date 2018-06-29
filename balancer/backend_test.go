package balancer

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("redisBackend", func() {
	var subject *redisBackend

	BeforeEach(func() {
		subject = newRedisBackend(&Options{
			Addr:    "127.0.0.1:7481",
			Network: "tcp",
			Rise:    2})
	})

	AfterEach(func() {
		Expect(subject.Close()).NotTo(HaveOccurred())
	})

	It("should ping periodically", func() {
		Expect(subject.Up()).To(BeTrue())
		Expect(subject.Down()).To(BeFalse())
		Expect(subject.Connections()).To(BeNumerically(">", 0))
		Expect(subject.Connections()).To(BeNumerically("<", 20000))
		Expect(subject.Latency()).To(BeNumerically(">", 0))
		Expect(subject.Latency()).To(BeNumerically("<", time.Second))
	})

	It("should update status based on rise/fall", func() {
		Expect(subject.Up()).To(BeTrue())
		subject.updateStatus(false)
		Expect(subject.Up()).To(BeFalse())
		for i := 0; i < 100; i++ {
			subject.updateStatus(false)
		}
		Expect(subject.Up()).To(BeFalse())
		subject.updateStatus(true)
		Expect(subject.Up()).To(BeFalse())
		subject.updateStatus(true)
		Expect(subject.Up()).To(BeTrue())
	})

})
