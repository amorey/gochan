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
	"github.com/amorey/gochan/internal/chancore"
)

type shared[T any] struct {
	ch chan T // the buffered channel; closed by Sender.Close
	// sendMu serializes the send-side critical section with Sender.Close.
	// Holding it around the blocking select guarantees Sender.Close cannot
	// race close(s.ch) with a pending `s.ch <- v` arm — once dead is closed,
	// the blocked sender wakes through that arm and releases sendMu before
	// close(s.ch) runs.
	sendMu   sync.Mutex
	chClosed atomic.Bool

	mu      sync.Mutex // guards the fields below
	rxCount int        // number of still-open receivers

	rxReady *chancore.CloseOnce // closed when the first Receiver is registered
	dead    *chancore.CloseOnce // closed when rxCount drops to zero or Hub.Close fires

	send chancore.BufferedSend[T]
	recv chancore.BufferedRecv[T]
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
		rxReady: chancore.NewCloseOnce(),
		dead:    chancore.NewCloseOnce(),
	}
	s.send = chancore.BufferedSend[T]{
		Ch:       s.ch,
		Dead:     s.dead.Done(),
		Ready:    s.rxReady,
		ChClosed: &s.chClosed,
		SendLock: &s.sendMu,
	}
	s.recv = chancore.BufferedRecv[T]{
		Ch:   s.ch,
		Dead: s.dead.Done(),
	}
	return &Hub[T]{s: s, tx: &Sender[T]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return
// the same handle. If the hub has been closed (explicitly or because
// every previously-registered receiver has already closed) the returned
// handle reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() gochan.Sender[T] { return h.tx }

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
	if s.dead.IsClosed() {
		return &Receiver[T]{s: s, closed: true}
	}
	if s.rxCount == 0 {
		s.rxReady.Close()
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
	s.dead.Close()
	s.mu.Unlock()
	h.tx.Close()
}

// Send enqueues v, blocking while the buffer is full and no receiver is
// ready to consume it. Returns [gochan.ErrClosed] if the sender has been
// closed, every receiver has been closed, or the hub has been closed.
func (tx *Sender[T]) Send(v T) error { return tx.s.send.Send(v) }

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is full
// and no receiver is currently parked on a recv, [gochan.ErrClosed] if
// closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error { return tx.s.send.TrySend(v) }

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	return tx.s.send.SendContext(ctx, v)
}

// Close closes the sender. Already-queued values remain receivable via
// Recv and Chan; receivers drain them in order before observing
// [gochan.ErrClosed]. Further Send calls return ErrClosed. Idempotent.
// Intended to be called by the single producer — spmc does not
// synchronize concurrent callers on the sender side (though Hub.Receiver
// is safe to call from any goroutine).
func (tx *Sender[T]) Close() { tx.s.send.CloseCh(&tx.s.sendMu) }

// Recv blocks until a value is available to this receiver. Returns the
// next value in the shared FIFO, or [gochan.ErrClosed] if the buffer is
// empty and the sender has closed, this receiver is closed, or the hub
// has been closed.
func (rx *Receiver[T]) Recv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	return rx.s.recv.Recv()
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if
// empty and closed (or this receiver/the hub is closed).
func (rx *Receiver[T]) TryRecv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	return rx.s.recv.TryRecv()
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. Cancellation does not close this receiver.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	return rx.s.recv.RecvContext(ctx)
}

// Chan returns the underlying receive-only channel, shared across all
// receivers in the same hub — values delivered on it count against the
// single shared queue, so two receivers selecting on Chan simultaneously
// still see each value only once. Closed when the sender closes (either
// directly or via [Hub.Close]) and the buffer drains. Closing this
// receiver does not close the channel; use Recv/TryRecv if you need that
// signal. Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T { return rx.s.ch }

// Close closes this receiver only. Other receivers and the sender are
// unaffected — they continue to consume and produce. Subsequent
// Recv/TryRecv/RecvContext calls on this handle return [gochan.ErrClosed].
// The sender only observes ErrClosed once every receiver has been closed
// (or the hub itself is closed). Idempotent.
func (rx *Receiver[T]) Close() {
	if rx.closed {
		return
	}
	rx.closed = true
	s := rx.s
	s.mu.Lock()
	s.rxCount--
	if s.rxCount == 0 {
		s.dead.Close()
	}
	s.mu.Unlock()
}
