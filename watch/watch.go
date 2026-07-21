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
// read. Closing the sender (via [Sender.Close]) does not immediately
// fail in-flight Recv calls on receivers that have not yet observed
// the final value: each such receiver gets one more Recv returning
// the latest value before subsequent calls return [gochan.ErrClosed].
// Closing a receiver, by contrast, fails further Recv calls on that
// handle immediately — any unseen pending value is abandoned. Closing
// the hub (via [Hub.Close]) is hard tear-down: the sender and every
// live receiver are closed immediately, with no final-value drain —
// use [Sender.Close] if you need the soft path.
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
// late subscribers only future values, a watch receiver's first Recv
// returns whatever value is current at the time of that first Recv (not
// at the time of registration — there is no snapshot taken when
// [Hub.Receiver] returns). There is no concept of "missed values" —
// only "the latest value, when you ask for it."
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
// Hub close-all is hard tear-down. [Hub.Close] closes the sender and
// every live receiver immediately. Recv / TryRecv / RecvContext
// return [gochan.ErrClosed] without delivering the current value,
// Chan feeders exit and close their channel, and future
// [Hub.Receiver] calls return pre-closed handles. Use [Sender.Close]
// if you want subscribers to observe the latest value once before
// shutdown (the standard "final state" pattern — shutdown signals
// carrying a final reason, last-known-good config on close, etc.).
// A receiver obtained from a hub whose sender has been closed via
// [Sender.Close] (but not [Hub.Close]) still delivers the final
// value once before ErrClosed.
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

	mu        sync.Mutex
	val       T
	notify    chan struct{} // closed (and replaced) on every state change
	waiters   int           // parked receivers; gates notify-channel allocation
	receivers map[*Receiver[T]]struct{}
	hubClosed bool
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

	// forTestingFeederParked, if non-nil, is invoked by the Chan feeder
	// goroutine each time it snapshots a value and enters the send
	// select. Tests use it to deterministically interleave a second
	// Send between the snapshot and the delivery; it is nil in
	// production builds.
	forTestingFeederParked func()

	// forTestingBeforeRecvLock and forTestingBeforeTryRecvLock, if
	// non-nil, are invoked after the lock-free closed/version checks
	// and before taking s.mu. Tests use them to deterministically
	// exercise the receiver-close re-checks under s.mu.
	forTestingBeforeRecvLock    func()
	forTestingBeforeTryRecvLock func()
}

// New creates a watch Hub seeded with initial as the current value.
// A Receiver obtained from this hub returns initial as its first Recv
// result unless the sender has published a newer value by the time
// that first Recv runs, in which case the receiver sees the latest
// value at read time. Registration via [Hub.Receiver] does not
// snapshot the slot.
func New[T any](initial T) *Hub[T] {
	s := &shared[T]{
		val:       initial,
		notify:    make(chan struct{}),
		receivers: make(map[*Receiver[T]]struct{}),
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
// first Recv returns whatever value is current at the time of that
// first Recv (not at the time of this call — registration does not
// snapshot the slot); subsequent Recv calls block until the value
// changes again. If the hub has already been closed via [Hub.Close]
// the returned handle is pre-closed and reports [gochan.ErrClosed]
// on use. If only the sender has been closed (via [Sender.Close])
// the receiver still delivers the final value once before subsequent
// calls return ErrClosed.
func (h *Hub[T]) Receiver() *Receiver[T] {
	rx := &Receiver[T]{s: h.s}
	rx.done.Init()
	h.s.mu.Lock()
	if h.s.hubClosed {
		rx.done.Close()
	} else {
		h.s.receivers[rx] = struct{}{}
	}
	h.s.mu.Unlock()
	return rx
}

// Close closes the sender and every live receiver. Receivers see
// [gochan.ErrClosed] immediately — the final-value drain is not
// performed; use [Sender.Close] if you need subscribers to observe
// the latest value once before shutdown. Future [Hub.Receiver] calls
// return pre-closed handles. Idempotent.
func (h *Hub[T]) Close() {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hubClosed {
		return
	}
	s.hubClosed = true
	s.txClosed.Store(true)
	// Closing each receiver's done wakes its parked Recv/feed
	// directly, so there's no need to also fire s.notify.
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = nil
}

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

// SendContext behaves like Send, but reports an already-cancelled ctx
// instead of publishing. Send never blocks, so the context is only
// checked at entry.
//
// Precedence is closed > cancelled: a sender already closed on entry
// reports [gochan.ErrClosed] even for an already-cancelled ctx, since
// that is the durable answer and a retry with a fresh context would only
// return it again. A cancelled ctx on a live sender still reports
// ctx.Err(). Pinned by the root package's conformance table.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	// txClosed is the same atomic Send tests under mu, so this entry
	// check is exact. A close landing between here and Send is reported
	// by Send's own check, so nothing is lost by not holding mu.
	if tx.s.txClosed.Load() {
		return gochan.ErrClosed
	}
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
func (rx *Receiver[T]) Recv() (T, error) {
	return rx.recvLoop(context.Background())
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is
// cancelled first. Cancellation does not close this receiver.
//
// Precedence is closed > cancelled > value. A termination visible on
// entry — this receiver closed, the hub closed, or the sender closed
// with no unseen value left — reports [gochan.ErrClosed] even for an
// already-cancelled ctx. Otherwise a cancelled ctx reports ctx.Err()
// *even when a new version is pending*, leaving it unread rather than
// consuming it, so a subscriber looping on RecvContext still observes
// its own shutdown signal however fast the publisher runs. The pending
// value is not lost: the next Recv returns it, which is what preserves
// [Sender.Close]'s soft-drain contract.
//
// That also means ctx.Err() is not an end-of-stream: with the sender
// closed and a version still unseen, every RecvContext on a cancelled
// ctx reports ctx.Err() and none reaches [gochan.ErrClosed]. Only
// draining to ErrClosed deregisters a receiver on its own, so a caller
// that stops on ctx.Err() must [Receiver.Close] — otherwise the handle
// stays in the hub's notify cohort for its lifetime. `defer rx.Close()`
// covers this, as it does for any abandoned receiver.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	return rx.recvLoop(ctx)
}

// recvLoop is the shared blocking-recv implementation. Recv passes
// context.Background() to opt out of cancellation — Background's
// Done() returns nil, and a nil channel in a select arm is never
// selected, so the cancellation arm is a no-op on that path. An unseen
// version is delivered ahead of a sender-close so that hub-close /
// sender-close can drain the final value, while explicit receiver-close
// (via [Receiver.Close]) short-circuits because rx.done is set inside
// that path and the loop re-checks rx.done at the top of each iteration
// before consulting state.
//
// The whole closed > cancelled > value precedence is evaluated in one
// ordered run under mu, rather than split between a lock-free probe and
// the locked body. Two reasons. The terminal exit carries a tear-down
// obligation — dropping this receiver from s.receivers — that has to
// happen under the same lock that decided it was terminal; when the
// cancelled path had its own copy of that decision it silently grew its
// own bare return instead (fixed, and pinned by
// TestRecvContextCancelledCloseUnregisters). And the cancellation check
// must sit above the version read, or the only cancellation arm would be
// the <-ctxDone below, reachable only once parked — so a receiver looping
// on RecvContext against a publisher fast enough to keep a new version
// always pending would take the value return every iteration and never
// observe its own shutdown signal. Pinned by
// TestRecvContextCancelBeatsPendingValue.
//
// Recv passes context.Background(), whose Done() is nil; a nil channel in
// a select is never ready, so the cancellation check falls straight
// through to default on that path.
func (rx *Receiver[T]) recvLoop(ctx context.Context) (T, error) {
	var z T
	ctxDone := ctx.Done()
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
		if rx.forTestingBeforeRecvLock != nil {
			rx.forTestingBeforeRecvLock()
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
		// closed: the sender is gone and this receiver has seen the final
		// version, so nothing can arrive again. Equivalent to the old
		// post-read txClosed test — version <= lastSeen is exactly when
		// the read below would not have fired — but hoisted above the
		// cancellation check, which is what makes it outrank it. An unseen
		// version fails this and is still delivered by the read below,
		// which is what preserves Sender.Close's soft-drain contract.
		if rx.s.txClosed.Load() && rx.s.version.Load() <= rx.lastSeen {
			delete(rx.s.receivers, rx)
			rx.s.mu.Unlock()
			return z, gochan.ErrClosed
		}
		// cancelled: above the read, so a pending version cannot starve it.
		select {
		case <-ctxDone:
			rx.s.mu.Unlock()
			return z, ctx.Err()
		default:
		}
		// value: the newest version this receiver has not seen.
		if ver := rx.s.version.Load(); ver > rx.lastSeen {
			v := rx.s.val
			rx.lastSeen = ver
			rx.s.mu.Unlock()
			return v, nil
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
			// Terminal: deregister so a long-lived hub doesn't pin
			// an abandoned receiver after a Sender.Close soft drain.
			rx.s.mu.Lock()
			delete(rx.s.receivers, rx)
			rx.s.mu.Unlock()
			return z, gochan.ErrClosed
		}
		return z, gochan.ErrEmpty
	}
	if rx.forTestingBeforeTryRecvLock != nil {
		rx.forTestingBeforeTryRecvLock()
	}
	// Slow path: version moved past lastSeen on the fast-path load,
	// and version is monotonic, so val is guaranteed pending. Take
	// mu only to read val coherently with the writer.
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return z, gochan.ErrClosed
	}
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
			if rx.forTestingFeederParked != nil {
				rx.forTestingFeederParked()
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
			delete(s.receivers, rx)
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
	delete(rx.s.receivers, rx)
	rx.s.mu.Unlock()
}
