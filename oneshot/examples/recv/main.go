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

	// Worker computes a result and sends it once. Send returns immediately —
	// it does not block on a receiver — so the worker never leaks even if
	// the main goroutine has already moved on.
	go func() {
		time.Sleep(50 * time.Millisecond) // simulate work
		_ = tx.Send(42)
	}()

	// Bound the wait with a context. If the deadline fires first we close
	// the receiver to signal the worker that nobody will read its result.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	v, err := rx.RecvContext(ctx)
	if err != nil {
		// Possible errors:
		//   - context deadline / cancellation (we timed out)
		//   - gochan.ErrClosed (worker closed without sending)
		rx.Close() // graceful shutdown: drop any value the worker may still send
		if errors.Is(err, gochan.ErrClosed) {
			fmt.Println("worker cancelled before sending")
			return
		}
		fmt.Println("gave up waiting:", err)
		return
	}
	fmt.Println("got result:", v)
}
