// Package spmc provides a single-producer, multi-consumer FIFO queue.
//
// A [Hub] hands out one [Sender] and any number of [Receiver]s. The sender
// feeds values into a bounded buffer that is drained by the receivers,
// with each value delivered to exactly one receiver. Capacity behaves
// exactly like a Go buffered channel: New[T](0) is a rendezvous
// channel, New[T](n) allows n queued values before Send blocks.
//
// The send-side handle returned by [Hub.Sender] is a singleton; spmc is
// single-producer by design, and the implementation does not synchronize
// concurrent callers on the sender handle — Send/TrySend/SendContext/Close
// are intended to be driven by one producer goroutine. If you need
// multiple producers, use [github.com/amorey/gochan/mpmc]. [Hub.Receiver]
// is safe to call from any goroutine, but each *Receiver[T] is intended
// for use by a single consumer goroutine. [Hub.Close] calls Close on the
// sender and on every live receiver.
//
// Unlike [github.com/amorey/gochan/broadcast], every value goes to one
// receiver, not all of them — spmc is load distribution, not fan-out.
//
// # Typical uses
//
// Distributing work items to a pool of workers from a single dispatcher
// goroutine, parallelizing a CPU-bound pipeline stage, fanning a single
// input stream out across N consumers without duplication.
//
// # Semantics
//
// Single producer, multi consumer. The send-side handle returned by
// [Hub.Sender] is a singleton intended to be driven by one producer
// goroutine; the implementation does not synchronize concurrent callers
// on the sender handle. Any number of goroutines may each hold their
// own *Receiver[T] (obtained from [Hub.Receiver]) and call Recv/Close
// on it; the implementation does not synchronize concurrent callers on
// the same receiver handle — call [Hub.Receiver] once per worker. For
// multi-producer workloads use [github.com/amorey/gochan/mpmc].
//
// Each value to exactly one receiver. Values are not broadcast. A Send
// deposits one value into the shared queue, and the next Recv on any
// receiver removes it. Choice of receiver is non-deterministic and not
// guaranteed to be fair — it follows Go's channel-receive scheduling.
//
// FIFO ordering across the queue. The queue itself preserves send order,
// but because work is split across multiple consumers, any single receiver
// only sees a subsequence of the sends — interleaved with the work other
// receivers grabbed.
//
// Sender close drains. Closing the sender does not discard already-buffered
// values; receivers drain them in order before any of them observes
// [gochan.ErrClosed]. Closing a single receiver, by contrast, only affects
// that receiver; the queue keeps flowing through the remaining ones.
//
// All receivers closed ⇒ sender sees ErrClosed. If every receiver has been
// closed, the sender's next Send/TrySend/SendContext returns
// [gochan.ErrClosed] and the value is dropped. This is how a sender
// notices that there is nobody left to receive its work.
//
// Backpressure. A bounded buffer applies natural backpressure: when full,
// Send blocks until some receiver makes room. Use capacity == 0 for strict
// rendezvous handoff with no buffering — useful when you want producers to
// slow down to exactly the rate of the slowest combined consumer
// throughput.
//
// Hub close-all. [Hub.Close] calls Close on every live receiver and on the
// sender. Recv-style callers see [gochan.ErrClosed] immediately; Chan
// consumers drain remaining values before seeing channel-closed.
package spmc

import (
	"context"

	"github.com/amorey/gochan/mpmc"
)

// Hub is the construction handle for an spmc pipeline. Use [Hub.Sender] to
// obtain the singleton send-side handle and [Hub.Receiver] to spawn one or
// more receivers. [Hub.Close] is equivalent to calling Close on the sender
// and on every live receiver.
type Hub[T any] struct {
	h  *mpmc.Hub[T]
	tx *Sender[T] // singleton, eager-minted at construction
}

// Sender is the send-side handle of an spmc pipeline.
type Sender[T any] struct{ tx *mpmc.Sender[T] }

// Receiver is a receive-side handle of an spmc pipeline. Obtain receivers
// via [Hub.Receiver]. Each receiver carries its own done signal so that
// closing one parked receiver wakes only that goroutine without disturbing
// peers. See [Receiver.Close] for the close/in-flight Recv race semantics.
type Receiver[T any] struct{ rx *mpmc.Receiver[T] }

// New creates a fresh spmc Hub backed by a buffered Go channel of
// the given capacity. capacity == 0 yields a rendezvous channel where Send
// blocks until some receiver is ready. capacity < 0 panics.
//
// Receivers are obtained from the hub via [Hub.Receiver]; a freshly
// constructed hub has no receivers, so Send will block (or TrySend will
// report ErrNotReady) until at least one receiver is registered.
func New[T any](capacity int) *Hub[T] {
	if capacity < 0 {
		panic("spmc: negative capacity")
	}
	h := mpmc.New[T](capacity)
	return &Hub[T]{h: h, tx: &Sender[T]{tx: h.Sender()}}
}

// Sender returns the singleton send-side handle. Repeated calls return
// the same handle. The handle is intended to be driven by one producer
// goroutine — spmc is single-producer by design and does not synchronize
// concurrent callers on the sender; use [github.com/amorey/gochan/mpmc]
// for multi-producer workloads. If the hub has been closed (explicitly
// or because every previously-registered receiver has already closed)
// the returned handle reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() *Sender[T] { return h.tx }

// Receiver returns a new receive-side handle bound to the shared queue.
// Use this to add workers to the consumer pool. Each returned receiver
// has its own independent Close state but shares the queue with every
// other receiver. Safe to call concurrently. If the hub has been closed
// (explicitly or because every previously-registered receiver has already
// closed) the returned handle is pre-closed and reports [gochan.ErrClosed]
// on use.
func (h *Hub[T]) Receiver() *Receiver[T] { return &Receiver[T]{rx: h.h.Receiver()} }

// Close closes the hub by calling Close on every live receiver and on the
// sender. Order matters: receivers are closed first (so an in-flight Send
// escapes via the dead signal) and then the sender is closed (which
// closes the underlying channel). Idempotent. Must not be called
// concurrently with an active Send from a different goroutine — see
// [Sender.Close].
func (h *Hub[T]) Close() { h.h.Close() }

// Send enqueues v, blocking while the buffer is full and no receiver is
// ready to consume it. Returns [gochan.ErrClosed] if the sender has been
// closed, every receiver has been closed, or the hub has been closed.
func (tx *Sender[T]) Send(v T) error { return tx.tx.Send(v) }

// TrySend is non-blocking. Returns [gochan.ErrNotReady] if no receiver
// has yet been registered, [gochan.ErrFull] if the buffer is full and no
// receiver is currently parked on a recv, [gochan.ErrClosed] if closed,
// or nil on success.
func (tx *Sender[T]) TrySend(v T) error { return tx.tx.TrySend(v) }

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued. A close already visible on entry outranks
// an already-cancelled ctx and yields [gochan.ErrClosed]; once the call is
// parked, a close and a cancellation landing together resolve at random,
// so treat either error as terminal for this send.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	return tx.tx.SendContext(ctx, v)
}

// Close closes the sender. Already-queued values remain receivable via
// Recv and Chan; receivers drain them in order before observing
// [gochan.ErrClosed]. Further Send calls return ErrClosed. Idempotent.
// Intended to be called by the producer goroutine that owns this
// sender — spmc does not synchronize concurrent callers on the sender
// handle.
func (tx *Sender[T]) Close() { tx.tx.Close() }

// Recv blocks until a value is available to this receiver. Returns the
// next value in the shared FIFO, or [gochan.ErrClosed] if the buffer is
// empty and the sender has closed, this receiver is closed, or the hub
// has been closed. A close racing an in-flight Recv is ordered as
// documented on [mpmc.Receiver.Recv]: a close visible on entry wins, a
// value already in hand wins over one landing mid-call, and no value is
// dropped either way.
func (rx *Receiver[T]) Recv() (T, error) { return rx.rx.Recv() }

// TryRecv is non-blocking. Returns the next value if one is buffered,
// [gochan.ErrEmpty] if empty but still open, or [gochan.ErrClosed] if
// empty and closed (or this receiver/the hub is closed).
func (rx *Receiver[T]) TryRecv() (T, error) { return rx.rx.TryRecv() }

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. Cancellation does not close this receiver.
//
// Precedence is closed > cancelled > value. A termination visible on
// entry — this receiver closed, the hub closed, or the sender closed
// with the buffer drained — reports [gochan.ErrClosed] even for an
// already-cancelled ctx. Otherwise a cancelled ctx reports ctx.Err()
// *even when a value is buffered*, leaving that value in the queue for
// another receiver rather than consuming it, so a worker looping on
// RecvContext still observes its own shutdown signal under sustained
// load. To flush what is buffered after cancelling, loop on
// [Receiver.TryRecv] until it returns any error: [gochan.ErrEmpty] while
// the sender is still open, [gochan.ErrClosed] once it has closed and the
// buffer is drained. Waiting for ErrEmpty alone never ends against a
// closed sender.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	return rx.rx.RecvContext(ctx)
}

// Chan returns the underlying receive-only channel, shared across all
// receivers in the same hub — values delivered on it count against the
// single shared queue, so two receivers selecting on Chan simultaneously
// still see each value only once. Closed when the sender closes (either
// directly or via [Hub.Close]) and the buffer drains. Closing this
// receiver does not close the channel; use Recv/TryRecv if you need that
// signal. Repeated calls return the same channel.
func (rx *Receiver[T]) Chan() <-chan T { return rx.rx.Chan() }

// Close closes this receiver only. Other receivers and the sender are
// unaffected — they continue to consume and produce. Subsequent
// Recv/TryRecv/RecvContext calls on this handle return [gochan.ErrClosed].
// The sender only observes ErrClosed once every receiver has been closed
// (or the hub itself is closed). Idempotent.
//
// A blocking Recv that has already won the select race on a value at the
// instant Close runs returns that value successfully; the next call
// returns ErrClosed. Buffered values remain in FIFO order across a racing
// Close, and no value is delivered to a fully-closed handle.
func (rx *Receiver[T]) Close() { rx.rx.Close() }
