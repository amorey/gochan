// Package spmc provides a single-producer, multi-consumer FIFO queue.
//
// One [Sender] feeds values into a bounded buffer that is drained by any
// number of [Receiver]s, with each value delivered to exactly one
// receiver. Receivers are obtained via [Sender.Consumer]. Capacity
// behaves exactly like a Go buffered channel: NewBounded[T](0) is a
// rendezvous channel, NewBounded[T](n) allows n queued values before
// Send blocks.
//
// Exactly one goroutine should call Send/Close on the sender; Consumer
// is safe to call from any goroutine. Each *Receiver[T] is intended for
// use by a single consumer goroutine.
package spmc

import (
	"context"
	"sync"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	ch        chan T        // the buffered channel; closed by Sender.Close
	mu        sync.Mutex    // guards rxCount and the rxReady / rxAllDone close
	rxCount   int           // number of still-open receivers
	rxReady   chan struct{} // closed when the first Consumer is registered
	rxAllDone chan struct{} // closed when rxCount transitions to zero
}

// Sender is the send-side handle of an spmc pair. Use [Sender.Consumer]
// to spawn receivers.
type Sender[T any] struct {
	s      *shared[T]
	closed bool
}

// Receiver is a receive-side handle of an spmc pair. Obtain receivers
// via [Sender.Consumer].
type Receiver[T any] struct {
	s      *shared[T]
	closed bool
}

// NewBounded creates a fresh spmc sender backed by a buffered Go channel
// of the given capacity. capacity == 0 yields a rendezvous channel where
// Send blocks until some receiver is ready. capacity < 0 panics.
//
// Receivers are obtained from the sender via [Sender.Consumer]; a freshly
// constructed spmc has no receivers, so Send will block (or TrySend will
// report ErrFull) until at least one Consumer is registered.
func NewBounded[T any](capacity int) *Sender[T] {
	if capacity < 0 {
		panic("spmc: negative capacity")
	}
	s := &shared[T]{
		ch:        make(chan T, capacity),
		rxReady:   make(chan struct{}),
		rxAllDone: make(chan struct{}),
	}
	return &Sender[T]{s: s}
}

// Consumer returns a new receiver bound to the shared queue. Use this to
// add workers to the consumer pool. Each returned receiver has its own
// independent Close state but shares the queue and sender-close signal
// with every other receiver. Safe to call concurrently.
//
// If every previously-registered receiver has already been closed (so
// the sender has already observed ErrClosed), the returned receiver is
// pre-closed.
func (tx *Sender[T]) Consumer() *Receiver[T] {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.rxAllDone:
		return &Receiver[T]{s: s, closed: true}
	default:
	}
	if s.rxCount == 0 {
		close(s.rxReady)
	}
	s.rxCount++
	return &Receiver[T]{s: s}
}

// Send enqueues v, blocking while the buffer is full and no receiver is
// ready to consume it. Returns [gochan.ErrClosed] if the sender has been
// closed or every receiver has been closed.
func (tx *Sender[T]) Send(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	select {
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	default:
	}
	// Wait for at least one Consumer to be registered before touching the
	// buffer; otherwise a Send with capacity > 0 would silently enqueue work
	// for nobody.
	select {
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	case <-tx.s.rxReady:
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	}
}

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is full
// and no receiver is currently parked on a recv, [gochan.ErrClosed] if
// closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	select {
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	default:
	}
	select {
	case <-tx.s.rxReady:
	default:
		return gochan.ErrFull
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxAllDone:
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
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	default:
	}
	select {
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-tx.s.rxReady:
	}
	select {
	case tx.s.ch <- v:
		return nil
	case <-tx.s.rxAllDone:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the sender. Already-queued values remain receivable;
// receivers drain them in order before observing [gochan.ErrClosed].
// Further Send calls return [gochan.ErrClosed]. Idempotent.
func (tx *Sender[T]) Close() {
	if tx.closed {
		return
	}
	tx.closed = true
	close(tx.s.ch)
}

// Recv blocks until a value is available to this receiver. Returns the
// next value in the shared FIFO, or [gochan.ErrClosed] if the buffer is
// empty and the sender has closed, or this receiver is closed.
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
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if
// empty and closed (or this receiver is closed).
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
// first. Cancellation does not close this receiver.
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

// Chan returns the underlying receive-only channel, shared across all
// receivers in the same spmc — values delivered on it count against the
// single shared queue, so two receivers selecting on Chan simultaneously
// still see each value only once. Closed when the sender closes and the
// buffer drains. Closing this receiver does not close the channel; use
// Recv/TryRecv if you need that signal. Repeated calls return the same
// channel.
func (rx *Receiver[T]) Chan() <-chan T {
	return rx.s.ch
}

// Close closes this receiver only. Other receivers and the sender are
// unaffected — they continue to consume and produce. Subsequent
// Recv/TryRecv/RecvContext calls on this handle return [gochan.ErrClosed].
// The sender only observes ErrClosed once every receiver has been closed.
// Idempotent.
func (rx *Receiver[T]) Close() {
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if rx.closed {
		return
	}
	rx.closed = true
	s.rxCount--
	if s.rxCount == 0 {
		close(s.rxAllDone)
	}
}
