package mpsc_test

import (
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/mpsc"
)

// Producer hot-path benchmark for mpsc. The axis under test is the
// per-Send overhead added by the chMu RWMutex; we vary the number of
// concurrent producers (which is the load that actually exercises the
// lock) and the buffer capacity (which controls how often Send blocks
// vs. fast-paths through the buffer).
//
// Each sub-benchmark runs b.N total sends, fanned out across `producers`
// goroutines, and is drained by a single receiver goroutine. Time is
// reported per Send via b.ReportMetric so different `producers` counts
// stay comparable.
func BenchmarkProducerThroughput(b *testing.B) {
	producerCounts := []int{1, 2, 4, 8, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0) * 4}
	capacities := []int{0, 1, 64, 1024}

	for _, cap := range capacities {
		for _, p := range producerCounts {
			name := "cap=" + strconv.Itoa(cap) + "/producers=" + strconv.Itoa(p)
			b.Run(name, func(b *testing.B) {
				runProducer(b, cap, p)
			})
		}
	}
}

func runProducer(b *testing.B, capacity, producers int) {
	h := mpsc.New[int](capacity)

	// Pre-register one sender per producer goroutine. Hub.Sender is
	// thread-safe but it acquires a mutex internally — we don't want
	// that on the timed path.
	senders := make([]gochan.Sender[int], producers)
	for i := range senders {
		senders[i] = h.Sender()
	}
	rx := h.Receiver()

	// Distribute b.N sends across producers; the remainder goes to the
	// first producer.
	perProducer := b.N / producers
	remainder := b.N - perProducer*producers

	// Drain in parallel; the receiver is intentionally a no-op so the
	// benchmark isolates the send path. With capacity == 0 the receiver
	// is the bottleneck regardless; with cap > 0 the buffer absorbs
	// bursts and the send-path lock is what we're actually measuring.
	done := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			if _, err := rx.Recv(); err != nil {
				return
			}
		}
		close(done)
	}()

	var wg sync.WaitGroup
	wg.Add(producers)

	b.ResetTimer()
	for i := 0; i < producers; i++ {
		n := perProducer
		if i == 0 {
			n += remainder
		}
		tx := senders[i]
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				if err := tx.Send(j); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	<-done
	b.StopTimer()

	// Tear down so the goroutine doesn't leak between iterations.
	for _, tx := range senders {
		tx.Close()
	}
}
