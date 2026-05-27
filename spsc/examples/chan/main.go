// spsc/examples/chan demonstrates the Chan()-based API for a
// single-producer / single-consumer queue, composed with a
// cancellation channel via select.
//
// For queue-style packages (spsc, spmc, mpsc, mpmc) Chan() exposes the
// underlying buffered channel. It closes when the sender closes AND
// the buffer is empty — exactly like a built-in Go channel. Receiver.Close()
// does NOT close Chan(); to bail out early we use a separate cancel
// channel in a select.
//
// Run:
//
//	go run ./spsc/examples/chan
package main

import (
	"fmt"
	"time"

	"github.com/amorey/gochan/spsc"
)

func main() {
	tx, rx := spsc.New[int](4)
	// rx.Close() is idempotent and safe to call after clean end-of-stream.
	// Deferring guarantees it runs on every exit path. It doesn't close
	// the shared channel, but the producer will observe ErrClosed on its
	// next Send and stop producing.
	defer rx.Close()

	// Producer pushes a finite stream, then closes for clean end-of-stream.
	go func() {
		defer tx.Close()
		for i := 0; i < 10; i++ {
			if err := tx.Send(i); err != nil {
				// ErrClosed here means the consumer bailed out — stop producing.
				fmt.Println("send failed:", err)
				return
			}
		}
	}()

	// Separate cancel signal so the consumer can bail out of its
	// select without depending on the producer to close first. In this
	// example we wire it to a short timer to demonstrate graceful early exit.
	cancel := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(cancel)
	}()

	ch := rx.Chan()
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				// Sender closed and buffer drained: clean end-of-stream.
				fmt.Println("stream complete")
				return
			}
			fmt.Println("got:", v)
		case <-cancel:
			fmt.Println("cancelled")
			return
		}
	}
}
