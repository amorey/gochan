// spmc/examples/recv demonstrates the Recv()-based API for a
// single-producer / multi-consumer work-distribution queue.
//
// One dispatcher emits jobs; N workers compete for them — each job
// goes to exactly one worker. Graceful shutdown: when the producer
// is done it Closes the sender, workers drain the buffer, and each
// then sees ErrClosed and exits.
//
// Run:
//
//	go run ./spmc/examples/recv
package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/spmc"
)

func main() {
	const workers = 3
	hub := spmc.New[int](8)
	// hub.Close() is idempotent close-all: it tears down the sender and
	// every receiver the hub has handed out. Deferring guarantees cleanup
	// on every exit path, even after a clean tx.Close() / drain.
	defer hub.Close()

	// Stand up workers first so the buffer doesn't have to absorb the full
	// stream before anyone is consuming.
	var wg sync.WaitGroup
	for id := 0; id < workers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Closing the receiver on the way out is hygiene — it doesn't
			// close the shared buffer (other workers keep draining it), it
			// just unregisters this worker.
			defer rx.Close()
			for {
				job, err := rx.Recv()
				if err != nil {
					if !errors.Is(err, gochan.ErrClosed) {
						fmt.Printf("worker %d: %v\n", id, err)
					}
					return
				}
				fmt.Printf("worker %d ran job %d\n", id, job)
			}
		}(id)
	}

	// Producer: dispatch jobs, then close to signal end-of-stream.
	// Closing the sender does not abandon buffered jobs — workers drain
	// first, then see ErrClosed.
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
