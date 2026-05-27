// broadcast/examples/chan demonstrates the Chan()-based API for a
// fan-out channel, with subscribers composing Chan() with a cancel
// signal via select for graceful early shutdown.
//
// For per-receiver-feeder hubs (broadcast, watch), Chan() returns a
// PRIVATE channel fed by a per-subscriber goroutine. Receiver.Close()
// DOES close it (the feeder shuts down), and sender-close also closes
// it after the feeder drains its last value. Always Close the receiver
// when you stop reading — otherwise the feeder pins itself.
//
// Note: when using Chan() you cannot observe ErrLagged. Drops are
// silent — values overwritten in the ring before the subscriber drains
// them simply never appear on Chan(). Use Recv() if you need to know
// when a subscriber falls behind.
//
// Run:
//
//	go run ./broadcast/examples/chan
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gochan/broadcast"
)

func main() {
	const subscribers = 3
	hub := broadcast.New[int](8)
	// hub.Close() is idempotent close-all: it tears down the sender and
	// every subscriber (and its feeder) the hub has handed out. Deferring
	// guarantees cleanup on every exit path, even after a clean drain.
	defer hub.Close()

	cancel := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(cancel)
	}()

	var wg sync.WaitGroup
	for id := 0; id < subscribers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Closing the receiver shuts down the per-subscriber feeder.
			// This is required for clean teardown — without it, the feeder
			// goroutine keeps waiting for the next value forever.
			defer rx.Close()
			ch := rx.Chan()
			for {
				select {
				case v, ok := <-ch:
					if !ok {
						return // sender closed and feeder drained
					}
					// Simulate variable work per subscriber so each drains at
					// its own pace. Slow subscribers may silently miss values
					// (the ring overwrites them); use Recv() to detect that.
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					fmt.Printf("sub %d got %d\n", id, v)
				case <-cancel:
					return // graceful early exit; deferred Close stops the feeder
				}
			}
		}(id)
	}

	tx := hub.Sender()
	for i := 0; i < 10; i++ {
		if err := tx.Send(i); err != nil {
			// ErrClosed here means the hub was torn down — stop producing.
			fmt.Println("send failed:", err)
			break
		}
	}
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
