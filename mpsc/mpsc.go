// Package mpsc provides a multi-producer, single-consumer FIFO queue.
//
// A [Hub] hands out any number of [Sender] handles and a single
// [Receiver]. Senders feed values into a shared buffer drained by the
// receiver. Capacity behaves exactly like a Go buffered channel:
// New[T](0) is a rendezvous channel, New[T](n) allows n
// queued values before Send blocks.
//
// Any number of goroutines may each hold their own *Sender[T] and call
// Send/Close on it; the implementation does not synchronize concurrent
// callers on the *same* sender handle — call [Hub.Sender] once per
// producer goroutine. Exactly one goroutine should call Recv/Close on
// the receiver. [Hub.Close] is equivalent to calling Close on the
// receiver and on every live sender — don't call it concurrently with an
// active Send on any sender from another goroutine.
package mpsc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	ch chan T // the buffered channel; closed by the last Sender.Close or by Hub.Close
	// chMu serializes concurrent ch <- v operations (held as a read lock)
	// with close(ch) (held as a write lock). Holding RLock around the
	// blocking select guarantees Hub.Close cannot race close(s.ch) with a
	// pending `s.ch <- v` arm — once s.dead is closed, the blocked sender
	// wakes through that arm and releases the RLock before close(s.ch) runs.
	chMu sync.RWMutex
	// chClosed is set true while holding chMu.Lock(). It is atomic so the
	// send-side fast paths can check it without acquiring the lock.
	chClosed atomic.Bool

	mu         sync.Mutex
	txCount    int  // number of still-open senders
	txEver     bool // at least one sender has ever been registered
	dead       chan struct{}
	deadClosed bool
}

// Hub is the construction handle for an mpsc pipeline. Use [Hub.Sender]
// to spawn one or more send-side handles and [Hub.Receiver] to obtain
// the singleton receive-side handle. [Hub.Close] is equivalent to
// calling Close on every live sender and on the receiver.
type Hub[T any] struct {
	s  *shared[T]
	rx *Receiver[T] // the singleton receiver, returned by Hub.Receiver
}

// Sender is a send-side handle of an mpsc pipeline. Obtain senders via
// [Hub.Sender].
type Sender[T any] struct {
	s      *shared[T]
	closed atomic.Bool
}

// Receiver is the singleton receive-side handle of an mpsc pipeline.
type Receiver[T any] struct {
	s      *shared[T]
	closed atomic.Bool
}

// New creates a fresh mpsc Hub backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where
// every Send blocks until the receiver is ready. capacity < 0 panics.
//
// Senders are obtained from the hub via [Hub.Sender]; a freshly
// constructed hub has no senders, so Recv will block (or TryRecv will
// report ErrEmpty) until at least one producer is registered and sends a
// value. The "all senders closed ⇒ ErrClosed" rule only kicks in once at
// least one sender has been registered — a fresh hub is not implicitly
// closed.
func New[T any](capacity int) *Hub[T] {
	if capacity < 0 {
		panic("mpsc: negative capacity")
	}
	s := &shared[T]{
		ch:   make(chan T, capacity),
		dead: make(chan struct{}),
	}
	h := &Hub[T]{s: s}
	h.rx = &Receiver[T]{s: s}
	return h
}

// Sender returns a new send-side handle bound to the shared queue. Use
// this to add producers to the fan-in. Each returned sender has its own
// independent Close state but shares the queue and receiver-close signal
// with every other sender. Safe to call concurrently. If the hub has
// been closed (explicitly via [Hub.Close], or implicitly because every
// previously-registered sender has already closed) or the receiver has
// been closed, the returned handle is pre-closed and reports
// [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() gochan.Sender[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deadClosed || s.chClosed.Load() || (s.txEver && s.txCount == 0) {
		tx := &Sender[T]{s: s}
		tx.closed.Store(true)
		return tx
	}
	s.txCount++
	s.txEver = true
	return &Sender[T]{s: s}
}

// Receiver returns the singleton receive-side handle. Repeated calls
// return the same handle. After the hub has been closed (explicitly via
// [Hub.Close], or implicitly because every previously-registered sender
// has already closed) the returned handle reports [gochan.ErrClosed] on
// use.
func (h *Hub[T]) Receiver() gochan.Receiver[T] {
	return h.rx
}

// Close closes the hub by calling Close on every live sender and on the
// receiver. The receiver is closed first (so an in-flight Send escapes
// via the dead signal before the underlying channel is closed). For
// Recv-style callers buffered values are abandoned; Chan consumers can
// drain them before seeing channel-closed. Idempotent. Inherits the
// senders' close discipline — don't call concurrently with an active
// Send on any sender from another goroutine.
func (h *Hub[T]) Close() {
	// Receiver.Close raises the dead signal — that's all live senders need
	// to start returning ErrClosed, so we don't have to walk sender handles.
	h.rx.Close()
	// Then close ch directly so Chan consumers see channel-closed promptly.
	// chMu.Lock waits for any in-flight sender to bail via the dead arm
	// before close(ch) runs.
	s := h.s
	s.chMu.Lock()
	if !s.chClosed.Load() {
		s.chClosed.Store(true)
		close(s.ch)
	}
	s.chMu.Unlock()
}

// Send enqueues v, blocking while the buffer is full. Returns
// [gochan.ErrClosed] if this sender has been closed, the receiver has
// been closed, or the hub has been closed; on ErrClosed the value is
// dropped.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	if tx.closed.Load() {
		return gochan.ErrClosed
	}
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	s.chMu.RLock()
	defer s.chMu.RUnlock()
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

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is
// full, [gochan.ErrClosed] if closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error {
	s := tx.s
	if tx.closed.Load() {
		return gochan.ErrClosed
	}
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	s.chMu.RLock()
	defer s.chMu.RUnlock()
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

// SendContext blocks like Send but returns ctx.Err() if ctx is
// cancelled before the value is enqueued.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	s := tx.s
	if tx.closed.Load() {
		return gochan.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.chClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.dead:
		return gochan.ErrClosed
	default:
	}
	s.chMu.RLock()
	defer s.chMu.RUnlock()
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

// Close closes this sender only. Other senders continue to produce.
// Subsequent Send/TrySend/SendContext calls on this handle return
// [gochan.ErrClosed]. The receiver only observes ErrClosed (after
// draining) when every sender has been closed. Idempotent. Intended to
// be called by the producer goroutine that owns this sender — mpsc does
// not synchronize concurrent callers on the same sender handle.
func (tx *Sender[T]) Close() {
	if !tx.closed.CompareAndSwap(false, true) {
		return
	}
	s := tx.s
	s.mu.Lock()
	s.txCount--
	// CAS above gated entry, so this Sender was a registered handle
	// (pre-closed handles never incremented txCount and short-circuit).
	// That implies txEver was true; checking txCount alone suffices.
	last := s.txCount == 0
	s.mu.Unlock()
	if !last {
		return
	}
	s.chMu.Lock()
	if !s.chClosed.Load() {
		s.chClosed.Store(true)
		close(s.ch)
	}
	s.chMu.Unlock()
}

// Recv blocks until a value is available. Returns the next value in
// FIFO order, or [gochan.ErrClosed] if the buffer is empty and every
// sender has closed, this receiver is closed, or the hub has been
// closed.
func (rx *Receiver[T]) Recv() (T, error) {
	if rx.closed.Load() {
		var z T
		return z, gochan.ErrClosed
	}
	s := rx.s
	// Honor receiver-/hub-close over any buffered values.
	select {
	case <-s.dead:
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
	case <-s.dead:
		var z T
		return z, gochan.ErrClosed
	}
}

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if the buffer is empty but at least one sender is
// still open, or [gochan.ErrClosed] if the buffer is empty and all
// senders are closed (or this receiver/the hub is closed).
func (rx *Receiver[T]) TryRecv() (T, error) {
	if rx.closed.Load() {
		var z T
		return z, gochan.ErrClosed
	}
	s := rx.s
	select {
	case <-s.dead:
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

// RecvContext blocks like Recv but returns ctx.Err() if ctx is
// cancelled first. Cancellation does not close the receiver.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	if rx.closed.Load() {
		var z T
		return z, gochan.ErrClosed
	}
	s := rx.s
	// Prefer a ready value over a cancelled context, but honor close.
	select {
	case <-s.dead:
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
	case <-s.dead:
		var z T
		return z, gochan.ErrClosed
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	}
}

// Chan returns the underlying receive-only channel, suitable for use in
// select. Closed when every sender has closed and the buffer has
// drained (or when the hub is closed). Closing the receiver does not
// close this channel — use Recv/TryRecv if you need that signal.
// Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T {
	return rx.s.ch
}

// Close closes the receiver. Pending or future Send calls on any sender
// return [gochan.ErrClosed], and subsequent Recv/TryRecv/RecvContext
// calls return ErrClosed. Any values still buffered are abandoned for
// Recv-style callers, but remain receivable via Chan until every sender
// closes. Idempotent.
func (rx *Receiver[T]) Close() {
	if !rx.closed.CompareAndSwap(false, true) {
		return
	}
	s := rx.s
	s.mu.Lock()
	if !s.deadClosed {
		s.deadClosed = true
		close(s.dead)
	}
	s.mu.Unlock()
}
