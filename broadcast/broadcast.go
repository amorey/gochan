// Package broadcast provides a fan-out channel backed by a fixed-size
// ring buffer.
//
// A [Hub] hands out a singleton [Sender] and any number of [Receiver]s.
// Every value published through the sender is delivered to every live
// receiver — unlike spmc/mpmc, which distribute each value to a single
// consumer. The ring is bounded; slow receivers do not block the sender.
// When a write would wrap onto an unread slot, the oldest unread value
// is overwritten and the affected receiver sees [gochan.ErrLagged] on
// its next Recv before resuming from the oldest still-buffered value.
//
// Send never blocks: it always succeeds, overwrites, or returns
// [gochan.ErrClosed]. TrySend exposes the overwrite condition to the
// publisher by returning [gochan.ErrFull] when a write would evict an
// unread value, letting callers self-throttle or implement a
// drop-newest policy on top of the package's default drop-oldest.
//
// A freshly constructed hub has no receivers; values published before
// any [Hub.Receiver] call are dropped without being written to the ring.
// Late subscribers start at the sender's current write position — they
// do not see historical values.
//
// # Typical uses
//
// Event-stream fan-out (one producer, many listeners), configuration-
// change notifications, market-data ticks distributed to many strategies,
// WebSocket / SSE push systems where slow clients must not back up the
// publisher.
//
// Unlike spmc/mpmc, every receiver sees every value (unless there's lag).
// Unlike [github.com/amorey/gochan/watch], values are buffered (you see
// the last N, not just the most recent).
//
// # Semantics
//
// Shared singleton sender, fan-out delivery. The send-side handle
// returned by [Hub.Sender] is a singleton that is safe to share across
// goroutines: any number of publishers may call Send / TrySend /
// SendContext / Close concurrently on the same handle. This is an
// intentional exception to the "one handle, one goroutine" rule that
// applies to the queue-style packages (spsc, spmc, mpsc, mpmc): broadcast's
// Send never parks — it always returns immediately, overwriting on wrap —
// so there is no parked-Send-races-Close window for shared use to expose.
// Any number of goroutines may each hold their own *Receiver[T] (obtained
// from [Hub.Receiver]) and call Recv/Close on it; the implementation does
// not synchronize concurrent callers on the same receiver handle — call
// [Hub.Receiver] once per subscriber. Every value goes to every live
// receiver — this is fan-out, not load distribution. Use
// [github.com/amorey/gochan/spmc] or [github.com/amorey/gochan/mpmc] if
// you want each value delivered to exactly one consumer.
//
// Drop-oldest, with sender-observable pressure. The ring buffer holds
// at most capacity values. When Send wraps around onto a slot still
// holding an unread value, the unread value is overwritten and the
// affected receiver(s) see [gochan.ErrLagged] on their next Recv.
// TrySend exposes the same condition to the publisher: it returns
// [gochan.ErrFull] and refuses to write when an overwrite would occur,
// letting publishers self-throttle or drop-newest if they prefer.
//
// Late subscribers see only future values. A receiver obtained via
// [Hub.Receiver] starts at the sender's current write position. Values
// published before registration are not replayed. To make a value
// durable across a subscribe boundary, publish it again after
// subscription completes.
//
// Sender Send never blocks. By design, Send always returns immediately
// (success, overwrite, or [gochan.ErrClosed]). This is the inverse of
// the queue-style packages, where Send blocks under backpressure. If
// you want backpressure here, use TrySend + a back-off loop, or use
// [github.com/amorey/gochan/mpmc] instead.
//
// No empty-hub gating. Unlike spmc / mpmc, broadcast does not block Send
// on the first [Hub.Receiver] call. Values published with no subscribers
// are dropped without being written to the ring — subsequent subscribers
// start at "now" and don't see them. This package therefore never
// returns [gochan.ErrNotReady].
//
// Hub close-all. [Hub.Close] closes the sender and every live receiver
// immediately. Already-buffered values are not drained: the next Recv
// returns [gochan.ErrClosed], and Chan feeder goroutines unblock and
// close their channel without delivering remaining ring contents. Use
// [Sender.Close] instead if you want subscribers to finish consuming
// already-published values before seeing close.
//
// # Patterns
//
// Fire-and-forget telemetry — publisher doesn't care about lag,
// subscribers handle it:
//
//	hub := broadcast.New[Metric](1024)
//
//	tx := hub.Sender()
//	go func() {
//	    for m := range metricStream {
//	        tx.Send(m) // never blocks; overwrites on wrap
//	    }
//	    tx.Close()
//	}()
//
//	rx := hub.Receiver()
//	for {
//	    m, err := rx.Recv()
//	    var lagged gochan.ErrLagged
//	    switch {
//	    case errors.As(err, &lagged):
//	        log.Warn("dropped metrics", "missed", lagged.Missed)
//	        continue
//	    case errors.Is(err, gochan.ErrClosed):
//	        return
//	    }
//	    process(m)
//	}
//
// Sender-side observability — publisher tracks subscriber lag without
// changing delivery:
//
//	for state := range stateStream {
//	    if err := tx.TrySend(state); errors.Is(err, gochan.ErrFull) {
//	        metrics.Inc("broadcast.subscriber_lagging")
//	    }
//	    tx.Send(state) // commit anyway — drop-oldest is fine for this stream
//	}
//
// User-built drop-newest — preserve older values when the ring is full:
//
//	for evt := range events {
//	    if err := tx.TrySend(evt); errors.Is(err, gochan.ErrFull) {
//	        droppedCount.Add(1)
//	        continue // skip the new value; keep older ones in the ring
//	    }
//	}
//
// Self-throttling publisher — slow down when subscribers lag:
//
//	for v := range source {
//	    for tx.TrySend(v) != nil {
//	        time.Sleep(backoff)
//	    }
//	}
package broadcast

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/internal/chancore"
)

type shared[T any] struct {
	capacity uint64

	// writePos and txClosed are mirrored as atomics so [Receiver.TryRecv]
	// can short-circuit the "no data, still open" case without taking mu.
	// Both are still mutated under mu (atomic stores) so the ordering
	// against the buf write is well-defined: the atomic store of
	// writePos happens-after the slot write, and a concurrent atomic
	// load that observes the new writePos transitively observes the
	// slot write too.
	writePos atomic.Uint64
	txClosed atomic.Bool

	mu        sync.Mutex
	buf       []T
	notify    chan struct{} // closed (and replaced) on every state change
	receivers map[*Receiver[T]]struct{}
	waiters   int  // parked receivers; gates notify-channel allocation
	hubClosed bool // accessed only under mu

	// minPos / minCount / minStale form an incremental "minimum
	// receiver position" tracker so [Sender.TrySend] can decide
	// eviction in O(1) most of the time. minPos is only meaningful
	// when len(receivers) > 0. minCount is the number of live
	// receivers currently at minPos; when it drops to zero the
	// tracker is stale and a recompute is deferred to the next
	// TrySend.
	minPos   uint64
	minCount int
	minStale bool
}

// signalLocked wakes parked receivers if any. Caller must hold s.mu.
// Skips the close/realloc churn entirely when no receivers are parked
// — the next would-be parker will observe the updated state under mu
// before sleeping, so missing the signal is harmless.
func (s *shared[T]) signalLocked() {
	if s.waiters == 0 {
		return
	}
	close(s.notify)
	s.notify = make(chan struct{})
}

// recomputeMinLocked walks the receiver set to refresh minPos /
// minCount. Caller must hold s.mu and len(s.receivers) > 0.
func (s *shared[T]) recomputeMinLocked() {
	var newMin uint64 = ^uint64(0)
	var newCount int
	for rx := range s.receivers {
		if rx.pos < newMin {
			newMin = rx.pos
			newCount = 1
		} else if rx.pos == newMin {
			newCount++
		}
	}
	s.minPos = newMin
	s.minCount = newCount
	s.minStale = false
}

// leaveMinCohortLocked decrements the cohort at minPos when a
// receiver at that position either advances or closes. When the
// cohort empties the tracker is marked stale and the next TrySend
// triggers a single O(N) recompute. Caller must hold s.mu.
func (s *shared[T]) leaveMinCohortLocked(oldPos uint64) {
	if s.minStale || oldPos != s.minPos {
		return
	}
	s.minCount--
	if s.minCount == 0 {
		s.minStale = true
	}
}

// Hub is the construction handle for a broadcast pipeline.
type Hub[T any] struct {
	s  *shared[T]
	tx *Sender[T]
}

// Sender is the singleton send-side handle. Safe to share across
// goroutines.
type Sender[T any] struct{ s *shared[T] }

// Receiver is a receive-side handle. Each receiver tracks its own
// read position and lag accounting; a single *Receiver[T] is intended
// for use by one consumer goroutine.
type Receiver[T any] struct {
	s    *shared[T]
	pos  uint64
	done chancore.CloseOnce

	chOnce sync.Once
	ch     chan T
}

// New creates a broadcast Hub backed by a ring buffer of the given
// capacity. capacity <= 0 panics: a ring of size zero cannot hold a
// value across a sender→receiver handoff without blocking the sender,
// which contradicts the package's non-blocking promise.
func New[T any](capacity int) *Hub[T] {
	if capacity <= 0 {
		panic("broadcast: capacity must be positive")
	}
	s := &shared[T]{
		capacity:  uint64(capacity),
		buf:       make([]T, capacity),
		notify:    make(chan struct{}),
		receivers: make(map[*Receiver[T]]struct{}),
	}
	return &Hub[T]{s: s, tx: &Sender[T]{s: s}}
}

// Sender returns the singleton send-side handle. Repeated calls return
// the same handle. The handle is safe to share across goroutines (see
// the package doc for why broadcast is an exception to the "one handle,
// one goroutine" rule). After the hub has been closed the returned
// handle reports [gochan.ErrClosed] on use.
func (h *Hub[T]) Sender() *Sender[T] { return h.tx }

// Receiver returns a new subscriber bound to the ring. The receiver's
// read position is set to the sender's current write position — only
// values published after this call are delivered.
func (h *Hub[T]) Receiver() *Receiver[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	rx := &Receiver[T]{s: s, pos: s.writePos.Load()}
	rx.done.Init()
	if s.hubClosed || s.txClosed.Load() {
		rx.done.Close()
		return rx
	}
	if len(s.receivers) == 0 {
		s.minPos = rx.pos
		s.minCount = 1
		s.minStale = false
	} else if !s.minStale && rx.pos == s.minPos {
		s.minCount++
	}
	s.receivers[rx] = struct{}{}
	return rx
}

// Close closes the sender and every live receiver. Already-buffered
// values are not drained — receivers see [gochan.ErrClosed] on their
// next operation, and Chan feeder goroutines unblock. Future
// [Hub.Sender] calls return the closed singleton; future [Hub.Receiver]
// calls return pre-closed handles. Idempotent.
func (h *Hub[T]) Close() {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hubClosed {
		return
	}
	s.hubClosed = true
	s.txClosed.Store(true)
	// Closing each receiver's done wakes its parked Recv directly,
	// so there's no need to also fire the shared notify channel.
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = nil
	s.minCount = 0
	s.minStale = false
	// Drop payload references — buffered values are abandoned, and
	// holding them here would pin them for the hub's lifetime.
	clear(s.buf)
}

// Send publishes v to the ring and returns immediately. Send never
// blocks: if the ring is full of unread values, the oldest unread slot
// is overwritten and the receiver(s) holding that slot will see
// [gochan.ErrLagged] on their next Recv. Returns [gochan.ErrClosed] if
// the sender or hub has been closed; on ErrClosed the value is dropped.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return gochan.ErrClosed
	}
	// Skip the write when there are no subscribers: late receivers
	// start at the current writePos and never replay history, so a
	// stored value here would only pin the payload for GC and burn
	// a slot copy. writePos stays put for the same reason — there
	// is no observer that cares about its advance.
	if len(s.receivers) == 0 {
		return nil
	}
	wp := s.writePos.Load()
	s.buf[wp%s.capacity] = v
	s.writePos.Store(wp + 1)
	s.signalLocked()
	return nil
}

// TrySend is the pressure-aware variant of [Sender.Send]. Returns
// [gochan.ErrFull] — without writing — if publishing v would evict an
// unread value from at least one live receiver. Returns
// [gochan.ErrClosed] if closed. Returns nil on a successful write.
//
// TrySend is the entry point for senders that want to observe
// subscriber lag (for metrics, back-pressure, or self-throttling) or
// implement a drop-newest policy on top of the package's default
// drop-oldest behavior — see the package overview for patterns.
func (tx *Sender[T]) TrySend(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return gochan.ErrClosed
	}
	if len(s.receivers) == 0 {
		return nil
	}
	wp := s.writePos.Load()
	if wp >= s.capacity {
		if s.minStale {
			s.recomputeMinLocked()
		}
		if s.minPos <= wp-s.capacity {
			return gochan.ErrFull
		}
	}
	s.buf[wp%s.capacity] = v
	s.writePos.Store(wp + 1)
	s.signalLocked()
	return nil
}

// SendContext returns ctx.Err() if ctx is already cancelled; otherwise
// behaves like Send. Send never blocks, so there is nothing for
// cancellation to interrupt mid-call.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Send(v)
}

// Close closes the sender. Already-published values remain in the ring
// and remain receivable (subject to lag) until each receiver catches
// up. Further Send / TrySend / SendContext calls return
// [gochan.ErrClosed]. Idempotent.
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

// Recv blocks until the next value is available at this receiver's
// position, the sender has closed and this receiver has caught up, or
// this receiver / the hub is closed. Returns the next value, or one
// of:
//
//   - [gochan.ErrLagged] — the receiver fell more than capacity values
//     behind the sender; some values were overwritten. The error
//     carries Missed — the number of values dropped before the receiver
//     caught up. The receiver's position is reset to the oldest
//     still-buffered value; the next Recv resumes from there. The
//     receiver is still usable.
//   - [gochan.ErrClosed] — the sender or hub has closed and this
//     receiver has already drained everything still in the ring at or
//     after its position.
func (rx *Receiver[T]) Recv() (T, error) { return rx.recvLoop(nil) }

// RecvContext blocks like [Receiver.Recv] but returns ctx.Err() if ctx
// is cancelled first. A ready value or [gochan.ErrLagged] is preferred
// over a cancelled context.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	return rx.recvLoop(ctx)
}

// recvLoop blocks until a value is available, the receiver/sender is
// closed, or (when ctx != nil) ctx is cancelled. A nil channel in a
// select arm is never selected, so a nil ctx degenerates into the
// non-cancellable Recv path.
//
// `parked` carries across iterations so the waiters++ done before
// sleeping is matched by a waiters-- on the next iteration's lock,
// without an extra mu acquisition. Early exits (rx-close, ctx-cancel)
// drop through the defer to release the same accounting.
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
		v, err := rx.tryReadLocked()
		if err != gochan.ErrEmpty {
			rx.s.mu.Unlock()
			return v, err
		}
		if rx.s.txClosed.Load() {
			rx.unregisterLocked()
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

// TryRecv is non-blocking. Returns the next value, [gochan.ErrEmpty]
// if caught up, [gochan.ErrLagged] if behind, or [gochan.ErrClosed].
func (rx *Receiver[T]) TryRecv() (T, error) {
	var z T
	if rx.done.IsClosed() {
		return z, gochan.ErrClosed
	}
	// Fast path: no new data since rx last read. writePos and
	// txClosed are atomic, so we can short-circuit without taking
	// mu for the still-open case. rx.pos is owned by this goroutine.
	// The EOF branch falls through to the locked section so it can
	// unregister the receiver and release its slot in the ring.
	if rx.s.writePos.Load() == rx.pos && !rx.s.txClosed.Load() {
		return z, gochan.ErrEmpty
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	// rx.done can flip between the lock-free check above and acquiring mu;
	// Close holds mu before closing done, so a re-check here is race-free.
	if rx.done.IsClosed() {
		return z, gochan.ErrClosed
	}
	v, err := rx.tryReadLocked()
	if err == gochan.ErrEmpty && rx.s.txClosed.Load() {
		rx.unregisterLocked()
		return z, gochan.ErrClosed
	}
	return v, err
}

// tryReadLocked consumes one value or reports lag. Returns
// [gochan.ErrEmpty] when the receiver is caught up to the sender.
// Must be called with rx.s.mu held.
func (rx *Receiver[T]) tryReadLocked() (T, error) {
	var z T
	s := rx.s
	wp := s.writePos.Load()
	if wp > s.capacity {
		oldest := wp - s.capacity
		if rx.pos < oldest {
			missed := oldest - rx.pos
			oldPos := rx.pos
			rx.pos = oldest
			s.leaveMinCohortLocked(oldPos)
			return z, gochan.ErrLagged{Missed: missed}
		}
	}
	if rx.pos < wp {
		v := s.buf[rx.pos%s.capacity]
		oldPos := rx.pos
		rx.pos++
		s.leaveMinCohortLocked(oldPos)
		return v, nil
	}
	return z, gochan.ErrEmpty
}

// Chan returns a per-receiver native channel that yields successive
// values in order, silently advancing past lagged values. Closed
// when the sender closes and this receiver drains, or when this
// receiver is closed. Repeated calls return the same channel.
//
// Abandoning the channel without calling [Receiver.Close] pins the
// feeder goroutine — it will park forever waiting for the next value
// or close signal. Always Close the receiver when you stop reading.
func (rx *Receiver[T]) Chan() <-chan T {
	rx.chOnce.Do(func() {
		rx.ch = make(chan T)
		go func() {
			defer close(rx.ch)
			for {
				v, err := rx.Recv()
				if err != nil {
					var lagged gochan.ErrLagged
					if errors.As(err, &lagged) {
						continue
					}
					return
				}
				select {
				case rx.ch <- v:
				case <-rx.done.Done():
					return
				}
			}
		}()
	})
	return rx.ch
}

// Close closes this receiver only. Other receivers and the sender
// are unaffected. Idempotent. Closing the last receiver does not
// close the sender — broadcast lets the publisher keep writing with
// no subscribers.
func (rx *Receiver[T]) Close() {
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	rx.unregisterLocked()
}

// unregisterLocked removes rx from the hub bookkeeping and clears the
// ring if no live receivers remain. Idempotent — gated by rx.done.Close.
// Caller must hold rx.s.mu.
//
// Closing under mu means TrySend (also under mu) never observes a
// receiver whose done is closed but whose entry is still in
// s.receivers — otherwise a concurrent unsubscribe could leak a false
// ErrFull through the eviction check.
//
// Releases ring payloads once the last subscriber is gone: future Sends
// will skip writes until a new receiver registers, so the existing
// contents would otherwise pin references for the lifetime of the hub.
func (rx *Receiver[T]) unregisterLocked() {
	if !rx.done.Close() {
		return
	}
	s := rx.s
	delete(s.receivers, rx)
	s.leaveMinCohortLocked(rx.pos)
	if len(s.receivers) == 0 {
		clear(s.buf)
	}
}
