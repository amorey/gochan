// mpmc/examples/chan demonstrates the Chan()-based API for a
// multi-producer / multi-consumer queue. Workers compose the shared
// receive channel with a cancel signal via select for graceful early
// shutdown.
//
// As with the other queue-style hubs, Chan() exposes the shared
// buffered channel. Receiver.Close() does NOT close it — that's why
// the workers below also listen on a cancel channel.
//
// Run:
//
//	go run ./mpmc/examples/chan
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

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
	// every exit path, even after a clean drain.
	defer hub.Close()

	cancel := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(cancel)
	}()

	var workersWG sync.WaitGroup
	for id := 0; id < workers; id++ {
		rx := hub.Receiver()
		workersWG.Add(1)
		go func(id int) {
			defer workersWG.Done()
			defer rx.Close()
			ch := rx.Chan()
			for {
				select {
				case j, ok := <-ch:
					if !ok {
						return // all senders closed and buffer drained
					}
					fmt.Printf("worker %d ran job (src=%d,n=%d)\n", id, j.src, j.n)
				case <-cancel:
					return // graceful early exit
				}
			}
		}(id)
	}

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
