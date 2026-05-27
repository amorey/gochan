// oneshot/examples/chan demonstrates the Chan()-based API for a
// single-value handoff, composed with other events in a select.
//
// The worker computes a result and Sends it once. The main goroutine
// selects between the delivery channel and a timeout — if the timeout
// fires first it Closes the receiver, which is the graceful-shutdown
// signal to the worker.
//
// Run:
//
//	go run ./oneshot/examples/chan
package main

import (
	"fmt"
	"time"

	"github.com/amorey/gochan/oneshot"
)

func main() {
	tx, rx := oneshot.New[int]()
	// Both Closes are idempotent and safe after a successful Send/Recv,
	// so deferring them guarantees cleanup on every exit path. On the
	// timeout path, rx.Close() also tells a still-pending Send to drop
	// its value rather than block.
	defer tx.Close()
	defer rx.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := tx.Send(42); err != nil {
			// ErrClosed here means the receiver gave up (e.g. timed out)
			// before we sent — the value is dropped.
			fmt.Println("send failed:", err)
		}
	}()

	// Chan() exposes a native channel that yields the value once and
	// is then closed. If the pair is cancelled before a successful
	// Send, the channel closes empty (the zero-value receive with
	// ok==false signals "no value, channel done").
	select {
	case v, ok := <-rx.Chan():
		if !ok {
			fmt.Println("worker closed without sending")
			return
		}
		fmt.Println("got result:", v)
	case <-time.After(200 * time.Millisecond):
		fmt.Println("gave up waiting")
	}
}
