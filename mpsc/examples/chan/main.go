// mpsc/examples/chan demonstrates the Chan()-based API for a
// multi-producer / single-consumer fan-in queue, with the consumer
// composing the receive channel with a cancel signal via select.
//
// The receive channel closes only when every Sender has closed and
// the buffer is empty (the same as mpsc itself). To bail out earlier
// we use a separate cancel channel — Receiver.Close() does NOT close
// the underlying Chan() on queue-style hubs.
//
// Run:
//
//	go run ./mpsc/examples/chan
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

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
	// every exit path, even after all senders have closed cleanly.
	defer hub.Close()

	cancel := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(cancel)
	}()

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

	rx := hub.Receiver()
	defer rx.Close()
	ch := rx.Chan()
loop:
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				// Every producer closed and the buffer drained: clean exit.
				break loop
			}
			fmt.Printf("got event from producer %d: %d\n", ev.src, ev.n)
		case <-cancel:
			// Graceful early exit. The receiver-side Close (via defer) and
			// the producers seeing ErrClosed on their next Send finish the
			// teardown.
			break loop
		}
	}

	wg.Wait()
	fmt.Println("done")
}
