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
//
// # Typical uses
//
// Fan-in of events from many worker goroutines into a single aggregator,
// collecting results from a scatter of parallel tasks, funnelling
// log/metric/event streams to one writer.
//
// # Semantics
//
// Multi producer / single consumer. Any number of goroutines may each
// hold their own *Sender[T] (obtained from [Hub.Sender]) and call
// Send/Close on it. Exactly one goroutine should call Recv/Close on the
// receiver. The implementation does not synchronize concurrent callers
// on the same sender handle — call [Hub.Sender] once per producer
// goroutine.
//
// FIFO across the queue, not across producers. The queue itself preserves
// the order in which sends arrive at the underlying channel, but the
// relative ordering of sends from different producers is not defined — it
// depends on scheduling. Sends from a single producer remain in order
// with respect to each other.
//
// All senders closed ⇒ receiver drains, then sees ErrClosed. Once at
// least one sender has been registered, the receiver observes
// [gochan.ErrClosed] when both (a) every sender obtained from
// [Hub.Sender] has been closed and (b) the buffer is empty. A freshly
// constructed hub with zero senders ever registered is not treated as
// closed — Recv blocks waiting for the first producer. If you spawn N
// producers you must close all N — a forgotten Close on any one of them
// leaves the receiver waiting forever for an EOF that never arrives.
//
// Receiver close stops everything. Closing the receiver immediately fails
// every pending and future Send across all senders with
// [gochan.ErrClosed] and abandons any buffered values.
//
// Backpressure. A bounded buffer applies natural backpressure: when full,
// Send blocks until the consumer makes room. Use capacity == 0 for strict
// rendezvous handoff with no buffering.
//
// Hub close-all. [Hub.Close] calls Close on every live sender and on the
// receiver. Recv-style callers see [gochan.ErrClosed] immediately; Chan
// consumers drain remaining values before seeing channel-closed.
package mpsc

import (
	"context"

	"github.com/amorey/gochan/mpmc"
)

// Hub is the construction handle for an mpsc pipeline. Use [Hub.Sender]
// to spawn one or more send-side handles and [Hub.Receiver] to obtain
// the singleton receive-side handle. [Hub.Close] is equivalent to
// calling Close on every live sender and on the receiver.
type Hub[T any] struct {
	h  *mpmc.Hub[T]
	rx *Receiver[T] // singleton, eager-minted at construction
}

// Sender is a send-side handle of an mpsc pipeline. Obtain senders via
// [Hub.Sender]. Each sender is owned by a single producer goroutine.
type Sender[T any] struct{ tx *mpmc.Sender[T] }

// Receiver is the singleton receive-side handle of an mpsc pipeline.
type Receiver[T any] struct{ rx *mpmc.Receiver[T] }

// New creates a fresh mpsc Hub backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where
// every Send blocks until the receiver is ready. capacity < 0 panics.
//
// Senders are obtained from the hub via [Hub.Sender]; a freshly
// constructed hub has no senders, so Recv will block (or TryRecv will
// report ErrNotReady) until at least one producer is registered and sends a
// value. The "all senders closed ⇒ ErrClosed" rule only kicks in once at
// least one sender has been registered — a fresh hub is not implicitly
// closed.
func New[T any](capacity int) *Hub[T] {
	if capacity < 0 {
		panic("mpsc: negative capacity")
	}
	h := mpmc.New[T](capacity)
	return &Hub[T]{h: h, rx: &Receiver[T]{rx: h.Receiver()}}
}

// Sender returns a new send-side handle bound to the shared queue. Use
// this to add producers to the fan-in. Each returned sender has its own
// independent Close state but shares the queue and receiver-close signal
// with every other sender. Safe to call concurrently. If the hub has
// been closed (explicitly via [Hub.Close], or implicitly because every
// previously-registered sender has already closed) or the receiver has
// been closed, the returned handle is pre-closed and reports
// [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() *Sender[T] { return &Sender[T]{tx: h.h.Sender()} }

// Receiver returns the singleton receive-side handle. Repeated calls
// return the same handle. After the hub has been closed (explicitly via
// [Hub.Close], or implicitly because every previously-registered sender
// has already closed) the returned handle reports [gochan.ErrClosed] on
// use.
func (h *Hub[T]) Receiver() *Receiver[T] { return h.rx }

// Close closes the hub by calling Close on every live sender and on the
// receiver. The receiver is closed first (so an in-flight Send escapes
// via the dead signal before the underlying channel is closed). For
// Recv-style callers buffered values are abandoned; Chan consumers can
// drain them before seeing channel-closed. Idempotent. Inherits the
// senders' close discipline — don't call concurrently with an active
// Send on any sender from another goroutine.
func (h *Hub[T]) Close() { h.h.Close() }

// Send enqueues v, blocking while the buffer is full. Returns
// [gochan.ErrClosed] if this sender has been closed, the receiver has
// been closed, or the hub has been closed; on ErrClosed the value is
// dropped.
func (tx *Sender[T]) Send(v T) error { return tx.tx.Send(v) }

// TrySend is non-blocking. Returns [gochan.ErrFull] if the buffer is
// full, [gochan.ErrClosed] if closed, or nil on success.
func (tx *Sender[T]) TrySend(v T) error { return tx.tx.TrySend(v) }

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued. A close already visible on entry outranks
// an already-cancelled ctx and yields [gochan.ErrClosed]; once the call is
// parked, a close and a cancellation landing together resolve at random,
// so treat either error as terminal for this send.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	return tx.tx.SendContext(ctx, v)
}

// Close closes this sender only. Other senders continue to produce.
// Subsequent Send/TrySend/SendContext calls on this handle return
// [gochan.ErrClosed]. The receiver only observes ErrClosed (after
// draining) when every sender has been closed. Idempotent. Intended to
// be called by the producer goroutine that owns this sender — mpsc does
// not synchronize concurrent callers on the same sender handle.
func (tx *Sender[T]) Close() { tx.tx.Close() }

// Recv blocks until a value is available. Returns the next value in
// FIFO order, or [gochan.ErrClosed] if the buffer is empty and every
// sender has closed, this receiver is closed, or the hub has been
// closed. A close racing an in-flight Recv is ordered as documented on
// [mpmc.Receiver.Recv]: a close visible on entry wins, a value already in
// hand wins over one landing mid-call, and no value is dropped either way.
func (rx *Receiver[T]) Recv() (T, error) { return rx.rx.Recv() }

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrNotReady] if no sender has yet been registered,
// [gochan.ErrEmpty] if the buffer is empty but at least one sender is
// still open, or [gochan.ErrClosed] if the buffer is empty and all
// senders are closed (or this receiver/the hub is closed).
func (rx *Receiver[T]) TryRecv() (T, error) { return rx.rx.TryRecv() }

// RecvContext blocks like Recv but returns ctx.Err() if ctx is
// cancelled first. Cancellation does not close the receiver.
//
// Precedence is closed > cancelled > value. A termination visible on
// entry — this receiver closed, the hub closed, or every sender closed
// with the buffer drained — reports [gochan.ErrClosed] even for an
// already-cancelled ctx. Otherwise a cancelled ctx reports ctx.Err()
// *even when a value is buffered*, leaving that value in the queue
// rather than consuming it, so a worker looping on RecvContext still
// observes its own shutdown signal under sustained load. To flush what
// is buffered after cancelling, loop on [Receiver.TryRecv] until it
// returns any error: [gochan.ErrEmpty] while a sender is still open,
// [gochan.ErrClosed] once every sender has closed and the buffer is
// drained. Waiting for ErrEmpty alone never ends against closed
// senders.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	return rx.rx.RecvContext(ctx)
}

// Chan returns the underlying receive-only channel, suitable for use in
// select. Closed when every sender has closed and the buffer has
// drained (or when the hub is closed). Closing the receiver does not
// close this channel — use Recv/TryRecv if you need that signal.
// Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T { return rx.rx.Chan() }

// Close closes the receiver. Pending or future Send calls on any sender
// return [gochan.ErrClosed], and subsequent Recv/TryRecv/RecvContext
// calls return ErrClosed. Any values still buffered are abandoned for
// Recv-style callers, but remain receivable via Chan until every sender
// closes. Idempotent.
func (rx *Receiver[T]) Close() { rx.rx.Close() }
