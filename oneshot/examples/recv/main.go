// oneshot/examples/recv demonstrates the Recv()-based API for a
// single-value request/response handoff.
//
// A worker goroutine computes a result and Sends it once. The main
// goroutine uses RecvContext so it can bail out gracefully if the
// worker takes too long — closing the receiver tells the worker that
// its result is no longer wanted.
//
// Run:
//
//	go run ./oneshot/examples/recv
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amorey/gochan"
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

	// Worker computes a result and sends it once. Send returns immediately —
	// it does not block on a receiver — so the worker never leaks even if
	// the main goroutine has already moved on.
	go func() {
		time.Sleep(50 * time.Millisecond) // simulate work
		if err := tx.Send(42); err != nil {
			// ErrClosed here means the receiver gave up (e.g. timed out)
			// before we sent — the value is dropped.
			fmt.Println("send failed:", err)
		}
	}()

	// Bound the wait with a context.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	v, err := rx.RecvContext(ctx)
	if err != nil {
		// Possible errors:
		//   - context deadline / cancellation (we timed out)
		//   - gochan.ErrClosed (worker closed without sending)
		if errors.Is(err, gochan.ErrClosed) {
			fmt.Println("worker cancelled before sending")
			return
		}
		fmt.Println("gave up waiting:", err)
		return
	}
	fmt.Println("got result:", v)
}
