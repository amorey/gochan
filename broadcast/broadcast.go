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
// any [Hub.Receiver] call are written into the ring but never delivered.
// Late subscribers start at the sender's current write position — they
// do not see historical values.
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
	writePos  atomic.Uint64
	txClosed  atomic.Bool
	hubClosed atomic.Bool // Hub.Close was called — gates future Receiver()

	mu        sync.Mutex
	buf       []T
	notify    chan struct{} // closed (and replaced) on every state change
	receivers map[*Receiver[T]]struct{}
	waiters   int // parked receivers; gates notify-channel allocation

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

// noteAdvanceLocked is called when a receiver's pos increases. If the
// receiver was at minPos and was the last one there, the tracker is
// marked stale. Caller must hold s.mu.
func (s *shared[T]) noteAdvanceLocked(oldPos uint64) {
	if oldPos == s.minPos {
		s.minCount--
		if s.minCount == 0 {
			s.minStale = true
		}
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
	done *chancore.CloseOnce

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

func (h *Hub[T]) Sender() gochan.Sender[T] { return h.tx }

// Receiver returns a new subscriber bound to the ring. The receiver's
// read position is set to the sender's current write position — only
// values published after this call are delivered.
func (h *Hub[T]) Receiver() gochan.Receiver[T] {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	rx := &Receiver[T]{
		s:    s,
		pos:  s.writePos.Load(),
		done: chancore.NewCloseOnce(),
	}
	if s.hubClosed.Load() || s.txClosed.Load() {
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
	if s.hubClosed.Load() {
		return
	}
	s.hubClosed.Store(true)
	s.txClosed.Store(true)
	for rx := range s.receivers {
		rx.done.Close()
	}
	s.receivers = make(map[*Receiver[T]]struct{})
	s.minCount = 0
	s.minStale = false
	s.signalLocked()
}

func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return gochan.ErrClosed
	}
	wp := s.writePos.Load()
	s.buf[wp%s.capacity] = v
	s.writePos.Store(wp + 1)
	s.signalLocked()
	return nil
}

// TrySend returns [gochan.ErrFull] — without writing — if publishing
// v would evict an unread value from at least one live receiver.
func (tx *Sender[T]) TrySend(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed.Load() {
		return gochan.ErrClosed
	}
	wp := s.writePos.Load()
	if wp >= s.capacity && len(s.receivers) > 0 {
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

func (rx *Receiver[T]) Recv() (T, error) { return rx.recvLoop(nil) }

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
		v, err := rx.tryReadLocked()
		if err != gochan.ErrEmpty {
			rx.s.mu.Unlock()
			return v, err
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

// TryRecv is non-blocking. Returns the next value, [gochan.ErrEmpty]
// if caught up, [gochan.ErrLagged] if behind, or [gochan.ErrClosed].
func (rx *Receiver[T]) TryRecv() (T, error) {
	var z T
	if rx.done.IsClosed() {
		return z, gochan.ErrClosed
	}
	// Fast path: no new data since rx last read. writePos and
	// txClosed are atomic, so we can short-circuit without taking
	// mu. rx.pos is owned by this goroutine.
	if rx.s.writePos.Load() == rx.pos {
		if rx.s.txClosed.Load() {
			return z, gochan.ErrClosed
		}
		return z, gochan.ErrEmpty
	}
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	return rx.tryReadLocked()
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
			s.noteAdvanceLocked(oldPos)
			return z, gochan.ErrLagged{Missed: missed}
		}
	}
	if rx.pos < wp {
		v := s.buf[rx.pos%s.capacity]
		oldPos := rx.pos
		rx.pos++
		s.noteAdvanceLocked(oldPos)
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
	// Close under mu so TrySend (also under mu) never observes a
	// receiver whose done is closed but whose entry is still in
	// s.receivers — otherwise a concurrent unsubscribe could leak a
	// false ErrFull through the eviction check.
	if !rx.done.Close() {
		return
	}
	delete(s.receivers, rx)
	if !s.minStale && rx.pos == s.minPos {
		s.minCount--
		if s.minCount == 0 {
			s.minStale = true
		}
	}
}
