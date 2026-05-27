// mpsc/examples/recv demonstrates the Recv()-based API for a
// multi-producer / single-consumer fan-in queue.
//
// N producers each mint their own Sender from the hub and emit some
// events; one consumer drains every event into a single aggregator.
// Graceful shutdown: every producer closes its own Sender when it's
// done, and the consumer sees ErrClosed only after all senders have
// closed and the buffer is empty.
//
// Run:
//
//	go run ./mpsc/examples/recv
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/mpsc"
)

type event struct {
	src int
	n   int
}

func main() {
	const producers = 3
	hub := mpsc.New[event](16)
	// hub.Close() is idempotent close-all: it tears down every sender and
	// receiver the hub has handed out. Deferring guarantees cleanup on
	// every exit path. Note: we still rely on each producer closing its
	// own Sender (not hub.Close) to signal end-of-stream, so the consumer
	// drains the buffer before seeing ErrClosed.
	defer hub.Close()

	// Spawn producers. Each one owns its own Sender handle and Closes it
	// when finished; the consumer only sees ErrClosed once every Sender
	// has been closed.
	var wg sync.WaitGroup
	for id := 0; id < producers; id++ {
		tx := hub.Sender()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer tx.Close()
			for i := 0; i < 4; i++ {
				// Simulate variable work so producers interleave realistically.
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
				if err := tx.Send(event{src: id, n: i}); err != nil {
					// ErrClosed here means the consumer bailed out — stop producing.
					fmt.Printf("producer %d: send failed: %v\n", id, err)
					return
				}
			}
		}(id)
	}

	// Once every producer has closed its own Sender, the consumer sees
	// ErrClosed after draining the buffer.
	rx := hub.Receiver()
	defer rx.Close()
	for {
		ev, err := rx.Recv()
		if err != nil {
			if !errors.Is(err, gochan.ErrClosed) {
				fmt.Println("recv error:", err)
			}
			break
		}
		fmt.Printf("got event from producer %d: %d\n", ev.src, ev.n)
	}
	wg.Wait()
	fmt.Println("done")
}
