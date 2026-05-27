// spsc/examples/recv demonstrates the Recv()-based API for a
// single-producer / single-consumer queue.
//
// One producer pushes a fixed number of values into a buffered queue;
// one consumer Recv()s them in order. The graceful shutdown pattern is
// "sender closes, receiver drains": Sender.Close() flushes the buffer
// to the consumer before subsequent Recv calls return ErrClosed,
// mirroring close() on a built-in Go channel.
//
// Run:
//
//	go run ./spsc/examples/recv
package main

import (
	"errors"
	"fmt"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/spsc"
)

func main() {
	// Capacity of 4 means the sender can park 4 unread values before
	// Send blocks, applying natural backpressure on a slow consumer.
	tx, rx := spsc.New[int](4)
	// rx.Close() is idempotent and safe to call after end-of-stream.
	// Deferring guarantees it runs on every exit path.
	defer rx.Close()

	// Producer: send a finite stream, then close to flag end-of-stream.
	// Closing the sender does not abandon buffered values — the receiver
	// will drain them first.
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

	// Consumer: drain until the sender closes and the buffer empties.
	for {
		v, err := rx.Recv()
		if err != nil {
			if errors.Is(err, gochan.ErrClosed) {
				break // clean end-of-stream
			}
			fmt.Println("recv error:", err)
			break
		}
		fmt.Println("got:", v)
	}

	fmt.Println("done")
}
