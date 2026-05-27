// spmc/examples/chan demonstrates the Chan()-based API for a
// single-producer / multi-consumer work-distribution queue, with
// workers using select to compose the channel with a cancel signal.
//
// For queue-style hubs, Chan() exposes the underlying buffered channel
// (closes when the sender closes AND the buffer drains). Receiver.Close()
// does NOT close it — so an early-exit path needs an external cancel
// channel in the select.
//
// Run:
//
//	go run ./spmc/examples/chan
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/amorey/gochan/spmc"
)

func main() {
	const workers = 3
	hub := spmc.New[int](8)
	// hub.Close() is idempotent close-all: it tears down the sender and
	// every receiver the hub has handed out. Deferring guarantees cleanup
	// on every exit path, even after a clean tx.Close() / drain.
	defer hub.Close()

	// External cancel signal for early exit. Wired to a timer in this
	// example; in real code this is usually ctx.Done() from caller.
	cancel := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(cancel)
	}()

	var wg sync.WaitGroup
	for id := 0; id < workers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer rx.Close()
			ch := rx.Chan()
			for {
				select {
				case job, ok := <-ch:
					if !ok {
						// Sender closed and buffer drained: clean exit.
						return
					}
					fmt.Printf("worker %d ran job %d\n", id, job)
				case <-cancel:
					// Graceful early exit. Returning unregisters this worker
					// via the deferred rx.Close.
					return
				}
			}
		}(id)
	}

	tx := hub.Sender()
	for j := 0; j < 10; j++ {
		if err := tx.Send(j); err != nil {
			break
		}
	}
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
