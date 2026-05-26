// Package spsc provides a single-producer, single-consumer FIFO queue.
//
// One [Sender] feeds values in order to one [Receiver]. Capacity behaves
// exactly like a Go buffered channel: NewBounded[T](0) is a rendezvous
// channel, NewBounded[T](n) allows n queued values before Send blocks.
//
// Exactly one goroutine should call Send/Close on the sender, and exactly
// one goroutine should call Recv/Close on the receiver. The implementation
// does not synchronize multiple concurrent callers on the same side.
package spsc

import (
	"context"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	ch     chan T        // the buffered channel; closed by Sender.Close
	rxDone chan struct{} // closed by Receiver.Close
}

// Sender is the send-side handle of an spsc pair.
type Sender[T any] struct {
	s      *shared[T]
	closed bool
}

// Receiver is the receive-side handle of an spsc pair.
type Receiver[T any] struct {
	s      *shared[T]
	closed bool
}

// NewBounded creates a fresh spsc pair backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where Send
// blocks until a matching Recv is ready. capacity < 0 panics.
func NewBounded[T any](capacity int) (*Sender[T], *Receiver[T]) {
	if capacity < 0 {
		panic("spsc: negative capacity")
	}
	s := &shared[T]{
		ch:     make(chan T, capacity),
		rxDone: make(chan struct{}),
	}
	return &Sender[T]{s: s}, &Receiver[T]{s: s}
}

// Send enqueues v, blocking while the buffer is full. Returns
// [gochan.ErrClosed] if the sender or receiver has been closed.
func (tx *Sender[T]) Send(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	select {
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	default:
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	}
}

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is full,
// [gochan.ErrClosed] if closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	select {
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	default:
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	default:
		return gochan.ErrFull
	}
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	default:
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxDone:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the sender. Already-queued values remain receivable; a
// subsequent Recv drains them and then returns [gochan.ErrClosed]. Further
// Send calls return [gochan.ErrClosed]. Idempotent.
func (tx *Sender[T]) Close() {
	if tx.closed {
		return
	}
	tx.closed = true
	close(tx.s.ch)
}

// Recv blocks until a value is available. Returns the next value in FIFO
// order, or [gochan.ErrClosed] if the buffer is empty and the sender has
// closed, or the receiver itself is closed.
func (rx *Receiver[T]) Recv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	v, ok := <-rx.s.ch
	if !ok {
		var z T
		return z, gochan.ErrClosed
	}
	return v, nil
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if empty
// and closed (or the receiver is closed).
func (rx *Receiver[T]) TryRecv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	select {
	case v, ok := <-rx.s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	default:
		var z T
		return z, gochan.ErrEmpty
	}
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. Cancellation does not close the receiver.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	// Prefer a ready value over a cancelled context when both are available.
	select {
	case v, ok := <-rx.s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	default:
	}
	select {
	case v, ok := <-rx.s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	}
}

// Chan returns the underlying receive-only channel, suitable for use in
// select. It is closed when the sender closes and the buffer drains.
// Closing the receiver does not close this channel; use Recv/TryRecv if
// you need to observe receiver-close. Repeated calls return the same
// channel.
func (rx *Receiver[T]) Chan() <-chan T {
	return rx.s.ch
}

// Close closes the receiver. Pending or future Send calls return
// [gochan.ErrClosed], and subsequent Recv/TryRecv/RecvContext calls return
// [gochan.ErrClosed]. Any values still buffered are abandoned. Idempotent.
func (rx *Receiver[T]) Close() {
	if rx.closed {
		return
	}
	rx.closed = true
	close(rx.s.rxDone)
}
