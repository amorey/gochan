// Package spmc provides a single-producer, multi-consumer FIFO queue.
//
// A [Hub] hands out one [Sender] and any number of [Receiver]s. The sender
// feeds values into a bounded buffer that is drained by the receivers,
// with each value delivered to exactly one receiver. Capacity behaves
// exactly like a Go buffered channel: New[T](0) is a rendezvous
// channel, New[T](n) allows n queued values before Send blocks.
//
// Exactly one goroutine should call Send/Close on the sender; [Hub.Receiver]
// is safe to call from any goroutine. Each *Receiver[T] is intended for
// use by a single consumer goroutine. [Hub.Close] calls Close on the
// sender and on every live receiver — don't call it concurrently with an
// active Send from a different goroutine.
package spmc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	ch chan T // the buffered channel; closed by Sender.Close
	// sendMu serializes the send-side critical section (Send/TrySend/
	// SendContext) with Sender.Close. Holding it around the blocking
	// select guarantees Sender.Close cannot race close(s.ch) with a
	// pending `s.ch <- v` arm — once s.dead is closed, the blocked
	// sender wakes through that arm and releases sendMu before
	// close(s.ch) runs.
	sendMu sync.Mutex
	// chClosed is set true by Sender.Close while holding sendMu. It is
	// atomic so the send-side fast paths can check it without acquiring
	// sendMu (which would block Sender.Close while a producer is parked
	// waiting for rxReady or in the blocking select).
	chClosed   atomic.Bool
	mu         sync.Mutex    // guards the fields below
	rxCount    int           // number of still-open receivers
	rxReady    chan struct{} // closed when the first Receiver is registered
	dead       chan struct{} // closed when rxCount drops to zero or Hub.Close fires
	deadClosed bool
}

// Hub is the construction handle for an spmc pipeline. Use [Hub.Sender] to
// obtain the singleton send-side handle and [Hub.Receiver] to spawn one or
// more receivers. [Hub.Close] is equivalent to calling Close on the sender
// and on every live receiver.
type Hub[T any] struct {
	s  *shared[T]
	tx *Sender[T] // the singleton sender, returned by Hub.Sender
}

// Sender is the send-side handle of an spmc pipeline.
type Sender[T any] struct{ s *shared[T] }

// Receiver is a receive-side handle of an spmc pipeline. Obtain receivers
// via [Hub.Receiver].
type Receiver[T any] struct {
	s      *shared[T]
	closed bool
}

// New creates a fresh spmc Hub backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where Send
// blocks until some receiver is ready. capacity < 0 panics.
//
// Receivers are obtained from the hub via [Hub.Receiver]; a freshly
// constructed hub has no receivers, so Send will block (or TrySend will
// report ErrFull) until at least one receiver is registered.
func New[T any](capacity int) *Hub[T] {
	if capacity < 0 {
		panic("spmc: negative capacity")
	}
	s := &shared[T]{
		ch:      make(chan T, capacity),
		rxReady: make(chan struct{}),
		dead:    make(chan struct{}),
	}
	return &Hub[T]{s: s, tx: &Sender[T]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return
// the same handle. If the hub has been closed (explicitly or because
// every previously-registered receiver has already closed) the returned
// handle reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() gochan.Sender[T] {
	return h.tx
}

// Receiver returns a new receive-side handle bound to the shared queue.
// Use this to add workers to the consumer pool. Each returned receiver
// has its own independent Close state but shares the queue with every
// other receiver. Safe to call concurrently. If the hub has been closed
// (explicitly or because every previously-registered receiver has already
// closed) the returned handle is pre-closed and reports [gochan.ErrClosed]
// on use.
func (h *Hub[T]) Receiver() gochan.Receiver[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deadClosed {
		return &Receiver[T]{s: s, closed: true}
	}
	if s.rxCount == 0 {
		close(s.rxReady)
	}
	s.rxCount++
	return &Receiver[T]{s: s}
}

// Close closes the hub by calling Close on every live receiver and on the
// sender. Order matters: receivers are closed first (so an in-flight Send
// escapes via the dead signal) and then the sender is closed (which
// closes the underlying channel). Idempotent. Must not be called
// concurrently with an active Send from a different goroutine — see
// [Sender.Close].
func (h *Hub[T]) Close() {
	s := h.s
	s.mu.Lock()
	if !s.deadClosed {
		s.deadClosed = true
		close(s.dead)
	}
	s.mu.Unlock()
	h.tx.Close()
}

// Send enqueues v, blocking while the buffer is full and no receiver is
// ready to consume it. Returns [gochan.ErrClosed] if the sender has been
// closed, every receiver has been closed, or the hub has been closed.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	// Wait for at least one receiver to be registered before touching the
	// buffer; otherwise a Send with capacity > 0 would silently enqueue work
	// for nobody.
	select {
	case <-s.dead:
		return gochan.ErrClosed
	case <-s.rxReady:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.dead:
		return gochan.ErrClosed
	}
}

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is full
// and no receiver is currently parked on a recv, [gochan.ErrClosed] if
// closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	s := tx.s
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	select {
	case <-s.rxReady:
	default:
		return gochan.ErrFull
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.dead:
		return gochan.ErrClosed
	default:
		return gochan.ErrFull
	}
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	s := tx.s
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-s.rxReady:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.ch <- v:
		return nil
	case <-s.dead:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the sender. Already-queued values remain receivable via
// Recv and Chan; receivers drain them in order before observing
// [gochan.ErrClosed]. Further Send calls return ErrClosed. Idempotent.
// Intended to be called by the single producer — spmc does not
// synchronize concurrent callers on the sender side (though Hub.Receiver
// is safe to call from any goroutine).
func (tx *Sender[T]) Close() {
	s := tx.s
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.chClosed.Load() {
		return
	}
	s.chClosed.Store(true)
	close(s.ch)
}

// Recv blocks until a value is available to this receiver. Returns the
// next value in the shared FIFO, or [gochan.ErrClosed] if the buffer is
// empty and the sender has closed, this receiver is closed, or the hub
// has been closed.
func (rx *Receiver[T]) Recv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	// Honor hub-close over any buffered values still in ch.
	select {
	case <-rx.s.dead:
		var z T
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-rx.s.ch:
		if !ok {
			var z T
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-rx.s.dead:
		var z T
		return z, gochan.ErrClosed
	}
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if
// empty and closed (or this receiver/the hub is closed).
func (rx *Receiver[T]) TryRecv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	select {
	case <-rx.s.dead:
		var z T
		return z, gochan.ErrClosed
	default:
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
	// Honor hub-close over any buffered value, but prefer a ready value
	// over a cancelled context.
	select {
	case <-rx.s.dead:
		var z T
		return z, gochan.ErrClosed
	default:
	}
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
	case <-rx.s.dead:
		var z T
		return z, gochan.ErrClosed
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	}
}

// Chan returns the underlying receive-only channel, shared across all
// receivers in the same hub — values delivered on it count against the
// single shared queue, so two receivers selecting on Chan simultaneously
// still see each value only once. Closed when the sender closes (either
// directly or via [Hub.Close]) and the buffer drains. Closing this
// receiver does not close the channel; use Recv/TryRecv if you need that
// signal. Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T {
	return rx.s.ch
}

// Close closes this receiver only. Other receivers and the sender are
// unaffected — they continue to consume and produce. Subsequent
// Recv/TryRecv/RecvContext calls on this handle return [gochan.ErrClosed].
// The sender only observes ErrClosed once every receiver has been closed
// (or the hub itself is closed). Idempotent.
func (rx *Receiver[T]) Close() {
	if rx.closed {
		return
	}
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	rx.closed = true
	s.rxCount--
	if s.rxCount == 0 && !s.deadClosed {
		s.deadClosed = true
		close(s.dead)
	}
}
