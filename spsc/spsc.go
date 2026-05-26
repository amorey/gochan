// Package spsc provides a single-producer, single-consumer FIFO queue.
//
// [New] hands out a sender, a receiver, and a close function that
// calls Close on both. Values flow in order from sender to receiver.
// Capacity behaves exactly like a Go buffered channel: New[T](0)
// is a rendezvous channel, New[T](n) allows n queued values before
// Send blocks.
//
// Exactly one goroutine should call Send/Close on the sender, and exactly
// one goroutine should call Recv/Close on the receiver. The close
// function inherits the sender's close discipline — don't call it
// concurrently with an active Send from a different goroutine.
package spsc

import (
	"context"
	"sync"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	ch        chan T        // the buffered channel; closed by Sender.Close
	abandoned chan struct{} // closed by Receiver.Close
	// sendMu serializes the send-side critical section (Send/TrySend/
	// SendContext) with Sender.Close. Holding it around the blocking
	// select guarantees Sender.Close cannot race close(s.ch) with a
	// pending `s.ch <- v` arm — once Receiver.Close has closed
	// s.abandoned, the blocked sender wakes through that arm and
	// releases sendMu before close(s.ch) runs.
	sendMu   sync.Mutex
	chClosed bool // guarded by sendMu
	// rxMu guards abandonedClosed independently of sendMu so
	// Receiver.Close can run while a producer is parked under sendMu.
	rxMu            sync.Mutex
	abandonedClosed bool
}

// Sender is the send-side handle of an spsc pair.
type Sender[T any] struct{ s *shared[T] }

// Receiver is the receive-side handle of an spsc pair.
type Receiver[T any] struct{ s *shared[T] }

// New creates a fresh spsc pair backed by a buffered Go channel of
// the given capacity, returning a sender, a receiver, and a close
// function that calls Close on both (Receiver first, then Sender, so an
// in-flight Send escapes via the abandoned signal before the underlying
// channel is closed). capacity == 0 yields a rendezvous channel where
// Send blocks until a matching Recv is ready. capacity < 0 panics. The
// close function is idempotent and safe to defer.
func New[T any](capacity int) (*Sender[T], *Receiver[T], func()) {
	if capacity < 0 {
		panic("spsc: negative capacity")
	}
	s := &shared[T]{
		ch:        make(chan T, capacity),
		abandoned: make(chan struct{}),
	}
	tx := &Sender[T]{s: s}
	rx := &Receiver[T]{s: s}
	return tx, rx, func() {
		rx.Close()
		tx.Close()
	}
}

// Send enqueues v, blocking while the buffer is full. Returns
// [gochan.ErrClosed] if the sender or receiver has been closed; on
// ErrClosed the value is dropped.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	select {
	case <-s.abandoned:
		return gochan.ErrClosed
	default:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.abandoned:
		return gochan.ErrClosed
	}
}

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is full,
// [gochan.ErrClosed] if closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	s := tx.s
	select {
	case <-s.abandoned:
		return gochan.ErrClosed
	default:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.abandoned:
		return gochan.ErrClosed
	default:
		return gochan.ErrFull
	}
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	s := tx.s
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.abandoned:
		return gochan.ErrClosed
	default:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.abandoned:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the sender. Already-queued values remain receivable via
// Recv and Chan; a subsequent Recv drains them and then returns
// [gochan.ErrClosed]. Further Send calls return ErrClosed. Idempotent.
func (tx *Sender[T]) Close() {
	s := tx.s
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed {
		return
	}
	s.chClosed = true
	close(s.ch)
}

// Recv blocks until a value is available. Returns the next value in FIFO
// order, or [gochan.ErrClosed] if the buffer is empty and the sender has
// closed, or if the receiver has been closed.
func (rx *Receiver[T]) Recv() (T, error) {
	s := rx.s
	// Honor abandonment over any buffered values still in ch.
	select {
	case <-s.abandoned:
		var z T
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-s.abandoned:
		var z T
		return z, gochan.ErrClosed
	}
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if empty
// and closed (or the receiver has been closed).
func (rx *Receiver[T]) TryRecv() (T, error) {
	s := rx.s
	select {
	case <-s.abandoned:
		var z T
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-s.ch:
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
	s := rx.s
	// Prefer a ready value over a cancelled context when both are available,
	// but honor abandonment over a buffered value.
	select {
	case <-s.abandoned:
		var z T
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	default:
	}
	select {
	case v, ok := <-s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-s.abandoned:
		var z T
		return z, gochan.ErrClosed
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	}
}

// Chan returns the underlying receive-only channel, suitable for use in
// select. It is closed when the sender closes (either directly or via
// the close function returned by [New]) and the buffer drains.
// Closing the receiver does not close this channel; use Recv/TryRecv if
// you need to observe receiver-close. Repeated calls return the same
// channel.
func (rx *Receiver[T]) Chan() <-chan T {
	return rx.s.ch
}

// Close closes the receiver. Pending or future Send calls return
// [gochan.ErrClosed], and subsequent Recv/TryRecv/RecvContext calls return
// [gochan.ErrClosed]. Buffered values are abandoned for Recv-style
// callers, but remain receivable via Chan until the sender closes.
// Idempotent.
func (rx *Receiver[T]) Close() {
	s := rx.s
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	if s.abandonedClosed {
		return
	}
	s.abandonedClosed = true
	close(s.abandoned)
}
