// Package mpmc provides a multi-producer, multi-consumer FIFO queue.
//
// A [Hub] hands out any number of [Sender] handles and any number of
// [Receiver] handles. Senders feed values into a bounded buffer drained
// by the receivers, with each value delivered to exactly one receiver
// (load distribution, not fan-out — for fan-out use the broadcast
// package). Capacity behaves exactly like a Go buffered channel:
// New[T](0) is a rendezvous channel, New[T](n) allows n queued values
// before Send blocks.
//
// Any number of goroutines may each hold their own *Sender[T] and call
// Send/Close on it; the implementation does not synchronize concurrent
// callers on the same sender handle — call [Hub.Sender] once per
// producer goroutine. Same rule for receivers: each *Receiver[T] is
// intended for use by a single consumer goroutine. [Hub.Close] is
// equivalent to calling Close on every live sender and receiver — don't
// call it concurrently with an active Send on any sender from another
// goroutine.
package mpmc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/internal/chancore"
)

type shared[T any] struct {
	ch chan T // the buffered channel; closed by the last Sender.Close or by Hub.Close
	// chMu serializes concurrent ch <- v operations (held as a read lock)
	// with close(ch) (held as a write lock). Holding RLock around the
	// blocking select guarantees the closer cannot race close(s.ch) with a
	// pending `s.ch <- v` arm — once dead is closed, the blocked sender
	// wakes through that arm and releases the RLock before close(s.ch) runs.
	chMu     sync.RWMutex
	chClosed atomic.Bool

	dead    *chancore.CloseOnce // closed when rxCount drops to zero or Hub.Close fires
	rxReady *chancore.CloseOnce // closed when the first Receiver is registered
	txReady *chancore.CloseOnce // closed when the first Sender is registered

	mu      sync.Mutex // guards txCount and rxCount
	txCount int        // number of still-open senders
	rxCount int        // number of still-open receivers

	send chancore.BufferedSend[T]
	recv chancore.BufferedRecv[T]
}

// Hub is the construction handle for an mpmc pipeline. Use [Hub.Sender]
// to spawn one or more send-side handles and [Hub.Receiver] to spawn one
// or more receive-side handles. [Hub.Close] is equivalent to calling
// Close on every live sender and every live receiver.
type Hub[T any] struct {
	s *shared[T]
}

// Sender is a send-side handle of an mpmc pipeline. Obtain senders via
// [Hub.Sender]. Each sender is owned by a single producer goroutine; the
// closed flag is only touched by that owner so no atomic is required.
type Sender[T any] struct {
	s      *shared[T]
	closed bool
}

// Receiver is a receive-side handle of an mpmc pipeline. Obtain receivers
// via [Hub.Receiver]. Each receiver is owned by a single consumer
// goroutine; the closed flag is only touched by that owner so no atomic
// is required.
type Receiver[T any] struct {
	s      *shared[T]
	closed bool
}

// New creates a fresh mpmc Hub backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where
// every Send blocks until some receiver is ready. capacity < 0 panics.
//
// A freshly constructed hub has neither senders nor receivers. Send
// blocks until at least one Receiver is registered via [Hub.Receiver];
// Recv blocks (and TryRecv reports [gochan.ErrNotReady]) until at least
// one Sender is registered via [Hub.Sender] and a value is sent. The
// implicit-close rules only kick in once each side has had at least one
// registration — a fresh hub is not implicitly closed.
func New[T any](capacity int) *Hub[T] {
	if capacity < 0 {
		panic("mpmc: negative capacity")
	}
	s := &shared[T]{
		ch:      make(chan T, capacity),
		dead:    chancore.NewCloseOnce(),
		rxReady: chancore.NewCloseOnce(),
		txReady: chancore.NewCloseOnce(),
	}
	s.send = chancore.BufferedSend[T]{
		Ch:       s.ch,
		Dead:     s.dead.Done(),
		Ready:    s.rxReady,
		ChClosed: &s.chClosed,
		SendLock: s.chMu.RLocker(),
	}
	s.recv = chancore.BufferedRecv[T]{
		Ch:    s.ch,
		Dead:  s.dead.Done(),
		Ready: s.txReady,
	}
	return &Hub[T]{s: s}
}

// Sender returns a new send-side handle bound to the shared queue. Use
// this to add producers to the fan-in. Safe to call concurrently. If the
// hub has been closed (explicitly via [Hub.Close], implicitly because
// every previously-registered sender has already closed, or because
// every previously-registered receiver has already closed) the returned
// handle is pre-closed and reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() gochan.Sender[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead.IsClosed() || s.chClosed.Load() || (s.txReady.IsClosed() && s.txCount == 0) {
		return &Sender[T]{s: s, closed: true}
	}
	s.txCount++
	s.txReady.Close()
	return &Sender[T]{s: s}
}

// Receiver returns a new receive-side handle bound to the shared queue.
// Use this to add workers to the consumer pool. Safe to call
// concurrently. If the hub has been closed (or every previously-
// registered receiver has already been closed) the returned handle is
// pre-closed and reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Receiver() gochan.Receiver[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead.IsClosed() || (s.rxReady.IsClosed() && s.rxCount == 0) {
		return &Receiver[T]{s: s, closed: true}
	}
	s.rxCount++
	s.rxReady.Close()
	return &Receiver[T]{s: s}
}

// Close closes the hub by closing every live receiver and every live
// sender. Receivers are closed first (so an in-flight Send escapes via
// the dead signal before the underlying channel is closed), and then
// the channel is closed. For Recv-style callers buffered values are
// abandoned; Chan consumers can drain them before seeing channel-closed.
// Idempotent. Inherits the senders' close discipline — don't call
// concurrently with an active Send on any sender from another goroutine.
func (h *Hub[T]) Close() {
	s := h.s
	s.mu.Lock()
	s.dead.Close()
	s.mu.Unlock()
	s.send.CloseCh(&s.chMu)
}

// Send enqueues v, blocking while the buffer is full and (until the
// first receiver registers) while no receiver exists. Returns
// [gochan.ErrClosed] if this sender has been closed, every receiver has
// been closed, or the hub has been closed; on ErrClosed the value is
// dropped.
func (tx *Sender[T]) Send(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	return tx.s.send.Send(v)
}

// TrySend is non-blocking. Returns [gochan.ErrNotReady] if no receiver
// has yet been registered, [gochan.ErrFull] if the buffer is full and no
// receiver is currently parked on a recv, [gochan.ErrClosed] if closed,
// or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	return tx.s.send.TrySend(v)
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	if tx.closed {
		return gochan.ErrClosed
	}
	return tx.s.send.SendContext(ctx, v)
}

// Close closes this sender only. Other senders continue to produce.
// Subsequent Send/TrySend/SendContext calls on this handle return
// [gochan.ErrClosed]. Receivers only observe ErrClosed (after draining)
// when every sender has been closed. Idempotent. Intended to be called
// by the producer goroutine that owns this sender — mpmc does not
// synchronize concurrent callers on the same sender handle.
func (tx *Sender[T]) Close() {
	if tx.closed {
		return
	}
	tx.closed = true
	s := tx.s
	s.mu.Lock()
	s.txCount--
	last := s.txCount == 0
	s.mu.Unlock()
	if !last {
		return
	}
	s.send.CloseCh(&s.chMu)
}

// Recv blocks until a value is available to this receiver. Returns the
// next value in the shared FIFO, or [gochan.ErrClosed] if the buffer is
// empty and every sender has closed, this receiver is closed, or the
// hub has been closed.
func (rx *Receiver[T]) Recv() (T, error) {
	if rx.closed {
		var z T
		return z, gochan.ErrClosed
	}
	return rx.s.recv.Recv()
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrNotReady] if no sender has yet been registered,
// [gochan.ErrEmpty] if the buffer is empty but at least one sender is
// still open, or [gochan.ErrClosed] if the buffer is empty and every
// sender has closed (or this receiver/the hub is closed).
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
// still see each value only once. Closed when every sender has closed
// (directly or via [Hub.Close]) and the buffer drains. Closing this
// receiver does not close the channel; use Recv/TryRecv if you need
// that signal. Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T { return rx.s.ch }

// Close closes this receiver only. Other receivers and the senders are
// unaffected — they continue to consume and produce. Subsequent
// Recv/TryRecv/RecvContext calls on this handle return [gochan.ErrClosed].
// Senders only observe ErrClosed once every receiver has been closed
// (or the hub itself has been closed). Idempotent.
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
