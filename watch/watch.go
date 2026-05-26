// Package watch provides a single-producer, multi-consumer latest-value
// channel.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s.
// The hub maintains a single slot that holds the current value; each
// [Sender.Send] overwrites it and bumps a monotonic version. Receivers
// see the slot's contents, not a stream — if the sender publishes A,
// B, C in rapid succession and the receiver only calls Recv once
// afterwards, the receiver sees C. Intermediate values are silently
// dropped.
//
// The hub is seeded with an initial value at construction. A receiver
// obtained from a fresh hub returns that initial value on its first
// Recv without waiting for a send; a receiver obtained later returns
// whatever the sender has most recently published.
//
// Send never blocks: slow receivers cannot apply backpressure to the
// publisher — they just skip ahead to the newest value on their next
// read. Closing the sender does not immediately fail in-flight Recv
// calls on receivers that have not yet observed the final value: each
// such receiver gets one more Recv returning the latest value before
// subsequent calls return [gochan.ErrClosed]. Closing a receiver,
// by contrast, fails further Recv calls on that handle immediately —
// any unseen pending value is abandoned.
//
// # Typical uses
//
// Configuration / settings propagation, "current state" distribution
// (current leader, current connection status, current feature flags),
// shutdown / cancellation signals carrying a final state, low-frequency
// control-plane updates where consumers only care about the latest
// snapshot.
//
// Unlike [github.com/amorey/gochan/broadcast], only the latest value is
// retained — receivers that fall behind catch up to "now" rather than
// seeing each intermediate value. Unlike spmc/mpmc, every receiver sees
// its own copy of the value (it is not load-distributed).
//
// # Semantics
//
// Latest-value-only delivery. Watch maintains a single slot containing
// the "current" value. Each Send overwrites that slot. Receivers see
// the slot's contents, not a stream — if the sender publishes A, B, C
// in rapid succession and the receiver only calls Recv once afterwards,
// the receiver sees C (A and B are silently dropped). This is the
// intended behavior; use [github.com/amorey/gochan/broadcast] if you
// need every value.
//
// Initial value is part of the API. Every receiver's first Recv returns
// the current value without waiting — there is no "empty" state. This
// makes watch ideal for "current configuration / current state"
// patterns where new subscribers need to bootstrap immediately rather
// than waiting for the next change.
//
// Late subscribers see the current value. Unlike broadcast, which gives
// late subscribers only future values, a watch receiver registered at
// time T sees whatever value is current at time T as its first Recv
// result. There is no concept of "missed values" — only "the latest
// value, when you ask for it."
//
// Sender Send never blocks. By design, Send always returns immediately
// (success or [gochan.ErrClosed]). Slow receivers cannot apply
// backpressure to the publisher — they just skip ahead to the newest
// value when they next read.
//
// Sender close delivers the final value. Closing the sender does not
// immediately fail in-flight or future Recv calls on receivers that
// have not yet observed the final value: each receiver gets one more
// Recv returning the current value (if it had not already caught up)
// before subsequent calls return [gochan.ErrClosed]. This is the
// standard "final state" pattern — shutdown signals carrying a final
// reason, last-known-good config on close, etc.
//
// Hub close-all. [Hub.Close] closes the sender and locks out future
// [Hub.Receiver] calls. Live receivers observe sender-close through the
// normal drain path: those that had not yet observed the latest value
// may still receive it once via Recv / Chan; receivers already caught
// up see [gochan.ErrClosed] immediately. A receiver obtained from a
// hub that has already been closed delivers the final value once
// before ErrClosed.
package watch

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/internal/chancore"
)

type shared[T any] struct {
	// version is mirrored as an atomic so [Receiver.TryRecv] can
	// short-circuit the "no new value, still open" case without taking
	// mu. version is bumped under mu after val is written; the mu
	// release/acquire pair is what publishes val to other goroutines.
	version  atomic.Uint64
	txClosed atomic.Bool

	mu      sync.Mutex
	val     T
	notify  chan struct{} // closed (and replaced) on every state change
	waiters int           // parked receivers; gates notify-channel allocation
}

// signalLocked wakes parked receivers if any. Caller must hold s.mu.
func (s *shared[T]) signalLocked() {
	if s.waiters == 0 {
		return
	}
	close(s.notify)
	s.notify = make(chan struct{})
}

// Hub is the construction handle for a watch pipeline.
type Hub[T any] struct {
	s  *shared[T]
	tx *Sender[T]
}

// Sender is the singleton send-side handle. Safe to share across
// goroutines (see the package doc for why watch is an exception to
// the "one handle, one goroutine" rule).
type Sender[T any] struct{ s *shared[T] }

// Receiver is a receive-side handle. Each receiver tracks the version
// of the last value it observed so that successive Recv calls return
// only newly published values (or the current value on the first call
// after registration).
type Receiver[T any] struct {
	s        *shared[T]
	lastSeen uint64 // owned by the consumer goroutine
	done     chancore.CloseOnce

	chOnce sync.Once
	ch     chan T

	// testFeederParked, if non-nil, is invoked by the Chan feeder
	// goroutine each time it snapshots a value and enters the send
	// select. Tests use it to deterministically interleave a second
	// Send between the snapshot and the delivery; it is nil in
	// production builds.
	testFeederParked func()
}

// New creates a watch Hub seeded with initial as the current value.
// Every Receiver obtained from this hub returns initial as its first
// Recv result unless the sender has already published a newer value
// by the time the receiver registers, in which case the receiver sees
// that newer value.
func New[T any](initial T) *Hub[T] {
	s := &shared[T]{
		val:    initial,
		notify: make(chan struct{}),
	}
	s.version.Store(1)
	return &Hub[T]{s: s, tx: &Sender[T]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return
// the same handle. The handle is safe to share across goroutines (see
// the package doc for why watch is an exception to the "one handle,
// one goroutine" rule). After the hub has been closed the returned
// handle reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() *Sender[T] { return h.tx }

// Receiver returns a new receiver bound to the hub. The receiver's
// first Recv returns the hub's current value immediately; subsequent
// Recv calls block until the value changes again. If the hub has
// already been closed the receiver still delivers the final value
// once (its lastSeen=0 < version) before subsequent calls return
// [gochan.ErrClosed].
func (h *Hub[T]) Receiver() *Receiver[T] {
	rx := &Receiver[T]{s: h.s}
	rx.done.Init()
	return rx
}

// Close closes the sender. Live receivers that have not yet observed
// the latest value may still receive it once via Recv / TryRecv / Chan
// before subsequent operations return [gochan.ErrClosed]; receivers
// already caught up see ErrClosed immediately. Future Sender calls
// return the closed singleton; future Receiver calls return handles
// that deliver the final value once and then ErrClosed. Idempotent.
func (h *Hub[T]) Close() { h.tx.Close() }

// Send publishes v as the new current value. Never blocks. If a
// receiver has not yet observed the previous value, that value is
// overwritten and the receiver jumps straight to v on its next Recv
// — intermediate values are silently skipped.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return gochan.ErrClosed
	}
	s.val = v
	s.version.Add(1)
	s.signalLocked()
	return nil
}

// TrySend is equivalent to Send for watch: Send never blocks, so
// there is no separate non-blocking path. Provided to satisfy the
// common Sender interface.
func (tx *Sender[T]) TrySend(v T) error { return tx.Send(v) }

// SendContext returns ctx.Err() if ctx is already cancelled;
// otherwise behaves like Send. Send never blocks, so the context is
// only checked at entry.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Send(v)
}

// Close closes the sender. Receivers that have not yet observed the
// most recent value may still receive it once before seeing
// [gochan.ErrClosed]; receivers already caught up see ErrClosed
// immediately. Further Send / TrySend / SendContext calls return
// ErrClosed. Idempotent.
func (tx *Sender[T]) Close() {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return
	}
	s.txClosed.Store(true)
	s.signalLocked()
}

// Recv returns the current value the first time it is called on a
// fresh receiver, then blocks until the value changes again. If the
// sender publishes multiple times between consecutive Recv calls,
// only the most recent value is returned.
func (rx *Receiver[T]) Recv() (T, error) { return rx.recvLoop(nil) }

// RecvContext blocks like Recv but returns ctx.Err() if ctx is
// cancelled first.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	return rx.recvLoop(ctx)
}

// recvLoop is the shared blocking-recv implementation. A nil ctx
// degenerates into the non-cancellable Recv path (a nil channel in a
// select arm is never selected). The pending-value check sits before
// the receiver-closed check so that hub-close / sender-close can
// drain the final value, while explicit receiver-close (via
// [Receiver.Close]) short-circuits because rx.done is set inside
// that path and the loop re-checks rx.done at the top of each
// iteration before consulting state.
func (rx *Receiver[T]) recvLoop(ctx context.Context) (T, error) {
	var z T
	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}
	parked := false
	defer func() {
		if parked {
			rx.s.mu.Lock()
			rx.s.waiters--
			rx.s.mu.Unlock()
		}
	}()
	for {
		if rx.done.IsClosed() {
			return z, gochan.ErrClosed
		}
		rx.s.mu.Lock()
		if parked {
			rx.s.waiters--
			parked = false
		}
		// Re-check under mu so a Receiver.Close that wins the race
		// between the pre-lock IsClosed check and the lock cannot
		// hand a pending value to a closed receiver. Receiver.Close
		// takes mu before closing done, so this check is stable.
		if rx.done.IsClosed() {
			rx.s.mu.Unlock()
			return z, gochan.ErrClosed
		}
		if ver := rx.s.version.Load(); ver > rx.lastSeen {
			v := rx.s.val
			rx.lastSeen = ver
			rx.s.mu.Unlock()
			return v, nil
		}
		if rx.s.txClosed.Load() {
			rx.s.mu.Unlock()
			return z, gochan.ErrClosed
		}
		rx.s.waiters++
		parked = true
		notify := rx.s.notify
		rx.s.mu.Unlock()
		select {
		case <-notify:
		case <-rx.done.Done():
			return z, gochan.ErrClosed
		case <-ctxDone:
			return z, ctx.Err()
		}
	}
}

// TryRecv returns the current value if this receiver has not yet
// observed it, [gochan.ErrEmpty] if caught up, or [gochan.ErrClosed]
// if the receiver/hub is closed and the final value has been
// observed.
func (rx *Receiver[T]) TryRecv() (T, error) {
	var z T
	if rx.done.IsClosed() {
		return z, gochan.ErrClosed
	}
	// Fast path: no new value since rx last read. version and
	// txClosed are atomic, so we can short-circuit without taking
	// mu. rx.lastSeen is owned by this goroutine.
	if rx.s.version.Load() == rx.lastSeen {
		if rx.s.txClosed.Load() {
			return z, gochan.ErrClosed
		}
		return z, gochan.ErrEmpty
	}
	// Slow path: version moved past lastSeen on the fast-path load,
	// and version is monotonic, so val is guaranteed pending. Take
	// mu only to read val coherently with the writer.
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	v := rx.s.val
	rx.lastSeen = rx.s.version.Load()
	return v, nil
}

// Chan returns a per-receiver native channel that yields successive
// values as they become current. The channel is unbuffered, so a
// fast publisher does not produce a backlog: while the consumer is
// not reading, additional sends only update the shared slot and the
// in-flight value will be replaced by the latest on the feeder's
// next iteration. Repeated calls on the same receiver return the
// same channel. The channel is closed when the feeder observes
// receiver-close or sender/hub-close-with-no-pending-value.
//
// Abandoning the channel without calling [Receiver.Close] pins the
// feeder goroutine — it will park forever waiting for the next
// value. Always Close the receiver when you stop reading.
func (rx *Receiver[T]) Chan() <-chan T {
	rx.chOnce.Do(func() {
		rx.ch = make(chan T)
		go rx.feed()
	})
	return rx.ch
}

// feed is the Chan goroutine. It must preserve the latest-value
// coalescing guarantee even when the consumer is slow: a value is
// only marked seen after it is actually delivered, and while parked
// on the send the feeder watches the hub's notify channel so that a
// newer publication replaces the in-flight value instead of being
// observed as a stale intermediate.
func (rx *Receiver[T]) feed() {
	defer close(rx.ch)
	s := rx.s
	parked := false
	defer func() {
		if parked {
			s.mu.Lock()
			s.waiters--
			s.mu.Unlock()
		}
	}()
	for {
		if rx.done.IsClosed() {
			return
		}
		s.mu.Lock()
		if parked {
			s.waiters--
			parked = false
		}
		if rx.done.IsClosed() {
			s.mu.Unlock()
			return
		}
		ver := s.version.Load()
		if ver > rx.lastSeen {
			v := s.val
			notify := s.notify
			s.waiters++
			parked = true
			s.mu.Unlock()
			if rx.testFeederParked != nil {
				rx.testFeederParked()
			}
			select {
			case rx.ch <- v:
				rx.lastSeen = ver
			case <-notify:
				// New value published (or sender closed) while we
				// were trying to deliver v. Loop to re-snapshot
				// from the slot so the consumer's next read sees
				// the latest value, not v.
			case <-rx.done.Done():
				return
			}
			continue
		}
		if s.txClosed.Load() {
			s.mu.Unlock()
			return
		}
		s.waiters++
		parked = true
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-rx.done.Done():
			return
		}
	}
}

// Close closes this receiver only. Other receivers and the sender
// are unaffected. Subsequent Recv / TryRecv / RecvContext calls
// return [gochan.ErrClosed] — any unseen pending value is abandoned.
// Idempotent. Closing the last receiver does not close the sender;
// the sender keeps holding the current value and may continue to
// publish for future subscribers.
func (rx *Receiver[T]) Close() {
	// Close under mu so a concurrent Recv that has acquired mu first
	// cannot hand back a pending value to a now-closed receiver: the
	// recvLoop re-checks rx.done after taking mu, and that check is
	// stable as long as Close serializes through mu too.
	rx.s.mu.Lock()
	rx.done.Close()
	rx.s.mu.Unlock()
}
