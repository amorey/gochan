// broadcast/examples/recv demonstrates the Recv()-based API for a
// fan-out channel: every value goes to every live subscriber.
//
// One publisher emits a stream of events; N subscribers each see every
// event. Slow subscribers can be overrun by the ring buffer — Recv
// then returns ErrLagged with the number of values missed, and the
// subscriber resumes from the oldest still-buffered value. Graceful
// shutdown: publisher closes the sender; subscribers' feeders drain
// remaining buffered values and then Recv returns ErrClosed.
//
// Run:
//
//	go run ./broadcast/examples/recv
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/broadcast"
)

func main() {
	const subscribers = 3
	hub := broadcast.New[int](8)
	// hub.Close() is idempotent close-all: it tears down the sender and
	// every subscriber (and its feeder) the hub has handed out. Deferring
	// guarantees cleanup on every exit path, even after a clean drain.
	defer hub.Close()

	// Subscribers — each gets a copy of every event.
	var wg sync.WaitGroup
	for id := 0; id < subscribers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Closing the receiver shuts down its per-subscriber feeder
			// goroutine; without this the feeder would pin itself waiting
			// for the next value.
			defer rx.Close()
			for {
				v, err := rx.Recv()
				if err != nil {
					var lagged gochan.ErrLagged
					if errors.As(err, &lagged) {
						// We fell behind. The receiver remains usable and the
						// next Recv returns the oldest still-buffered value.
						fmt.Printf("sub %d lagged by %d\n", id, lagged.Missed)
						continue
					}
					if errors.Is(err, gochan.ErrClosed) {
						return // clean end-of-stream
					}
					fmt.Printf("sub %d: %v\n", id, err)
					return
				}
				// Simulate variable work per subscriber so each drains at
				// its own pace — slow subscribers will visibly lag.
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
				fmt.Printf("sub %d got %d\n", id, v)
			}
		}(id)
	}

	// Publisher: emit a stream, then close to signal end-of-stream.
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
