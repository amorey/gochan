// mpmc/examples/recv demonstrates the Recv()-based API for a
// multi-producer / multi-consumer queue.
//
// N producers feed jobs into one shared buffer; M workers compete for
// them — each job goes to exactly one worker (load distribution, not
// fan-out). Graceful shutdown: producers close their own Senders when
// done; workers drain remaining jobs and then see ErrClosed.
//
// Run:
//
//	go run ./mpmc/examples/recv
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/mpmc"
)

type job struct {
	src int
	n   int
}

func main() {
	const producers = 2
	const workers = 3
	hub := mpmc.New[job](16)
	// hub.Close() is idempotent close-all: it tears down every sender and
	// receiver the hub has handed out. Deferring guarantees cleanup on
	// every exit path. Note: we still rely on each producer closing its
	// own Sender (not hub.Close) to signal end-of-stream, so workers
	// drain the buffer before seeing ErrClosed.
	defer hub.Close()

	// Workers. Each owns its own Receiver; closing it just removes that
	// worker from the pool, the other workers keep draining.
	var workersWG sync.WaitGroup
	for id := 0; id < workers; id++ {
		rx := hub.Receiver()
		workersWG.Add(1)
		go func(id int) {
			defer workersWG.Done()
			defer rx.Close()
			for {
				j, err := rx.Recv()
				if err != nil {
					if !errors.Is(err, gochan.ErrClosed) {
						fmt.Printf("worker %d: %v\n", id, err)
					}
					return
				}
				fmt.Printf("worker %d ran job (src=%d,n=%d)\n", id, j.src, j.n)
			}
		}(id)
	}

	// Producers. Once every producer's Sender is closed and the buffer
	// drains, each worker's Recv returns ErrClosed and the worker exits.
	var prodWG sync.WaitGroup
	for id := 0; id < producers; id++ {
		tx := hub.Sender()
		prodWG.Add(1)
		go func(id int) {
			defer prodWG.Done()
			defer tx.Close()
			for i := 0; i < 5; i++ {
				// Simulate variable work so producers interleave realistically.
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
				if err := tx.Send(job{src: id, n: i}); err != nil {
					// ErrClosed here means all workers bailed out — stop producing.
					fmt.Printf("producer %d: send failed: %v\n", id, err)
					return
				}
			}
		}(id)
	}

	prodWG.Wait()
	workersWG.Wait()
	fmt.Println("done")
}
