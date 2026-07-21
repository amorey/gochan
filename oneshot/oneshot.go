// Package oneshot provides a single-value, single-delivery channel.
//
// Exactly one Send will ever succeed and the value is delivered to exactly
// one Recv. [New] hands out a sender and a receiver; each side is closed
// via its own [Sender.Close] / [Receiver.Close]. Either side may cancel
// by closing its handle, and the other side observes [gochan.ErrClosed]
// on its next operation.
//
// # Typical uses
//
// Returning a single result from a goroutine, request/response RPC-style
// handoff, "done" signalling with an attached value.
//
// # Semantics
//
// Single delivery. After one successful Send/Recv pair, both sides are
// spent. Subsequent Send returns [gochan.ErrClosed]; subsequent Recv
// returns [gochan.ErrClosed].
//
// Cancellation. Either side may Close to abandon the exchange. The other
// side observes [gochan.ErrClosed] on its next operation. Send returning
// nil means the value was accepted into the slot, not that a Recv has
// observed it — Send does not block on a receiver. If the receiver closes
// before consuming, the value is silently dropped and the receiver's next
// Recv returns [gochan.ErrClosed]. Send only returns ErrClosed when the
// pair was already terminal at the moment Send acquired the slot lock.
//
// No goroutine leak. Because Send does not block on a receiver, a sender
// that completes its work and then has its receiver vanish never leaks.
// Conversely, a Recv caller that wants to bail must use [Receiver.RecvContext]
// or [Receiver.Close].
package oneshot

import (
	"context"
	"sync"

	"github.com/amorey/gochan"
)

type shared[T any] struct {
	mu       sync.Mutex
	val      T
	hasVal   bool          // value sits in the slot, awaiting consumption
	txClosed bool          // sender closed without sending
	rxClosed bool          // receiver closed (or value already consumed)
	done     chan struct{} // closed by the first terminal event
	userCh   chan T        // returned by Chan(); lazily created
}

func (s *shared[T]) terminalLocked() bool {
	return s.hasVal || s.txClosed || s.rxClosed
}

// Sender is the send-side handle of a oneshot pair.
type Sender[T any] struct{ s *shared[T] }

// Receiver is the receive-side handle of a oneshot pair.
type Receiver[T any] struct{ s *shared[T] }

// New creates a fresh oneshot pair: a sender and a receiver. Each side
// is closed independently via [Sender.Close] / [Receiver.Close]; both
// are idempotent.
func New[T any]() (*Sender[T], *Receiver[T]) {
	s := &shared[T]{done: make(chan struct{})}
	return &Sender[T]{s: s}, &Receiver[T]{s: s}
}

// Send deposits v into the slot and returns immediately without waiting for a
// receiver. Returns [gochan.ErrClosed] if the pair is already terminated at
// the moment Send acquires the slot lock. A nil return means the value was
// accepted into the slot; a concurrent [Receiver.Close] that wins the race
// after Send commits may still drop the value before any Recv observes it
// (see the package overview).
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	s.mu.Lock()
	if s.terminalLocked() {
		s.mu.Unlock()
		return gochan.ErrClosed
	}
	userCh := s.userCh
	if userCh != nil {
		// Chan() was registered earlier; the receiver is parked on userCh,
		// not on the slot. Mark consumed under the lock so concurrent
		// Recv/TryRecv/Close see the terminal state, then deliver outside
		// the critical section. The cap-1 userCh has never been written to,
		// so the send below never blocks.
		s.rxClosed = true
	} else {
		s.val = v
		s.hasVal = true
	}
	s.mu.Unlock()
	if userCh != nil {
		userCh <- v
		close(userCh)
	}
	close(s.done)
	return nil
}

// TrySend is equivalent to Send: oneshot Send never blocks.
func (tx *Sender[T]) TrySend(v T) error { return tx.Send(v) }

// SendContext behaves like Send, but reports an already-cancelled ctx
// instead of sending. Send never blocks, so there is nothing for
// cancellation to interrupt mid-call.
//
// Precedence is closed > cancelled: a sender already closed on entry
// reports [gochan.ErrClosed] even for an already-cancelled ctx, since
// that is the durable answer and a retry with a fresh context would only
// return it again. A cancelled ctx on a live sender still reports
// ctx.Err(). Pinned by the root package's conformance table.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
	// s.done closes on the first terminal event of either side, which is
	// exactly when Send would report ErrClosed — so this is an exact
	// terminal test and needs no lock.
	select {
	case <-tx.s.done:
		return gochan.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Send(v)
}

// Close cancels the channel from the sender side. Idempotent; a no-op after a
// successful Send.
func (tx *Sender[T]) Close() {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalLocked() {
		return
	}
	s.txClosed = true
	if s.userCh != nil {
		close(s.userCh)
	}
	close(s.done)
}

// Recv blocks until the value is sent. Returns [gochan.ErrClosed] if the
// sender closes without sending, or if the value has already been consumed.
func (rx *Receiver[T]) Recv() (T, error) {
	<-rx.s.done
	return rx.consume()
}

// TryRecv is non-blocking. Returns the value if already sent,
// [gochan.ErrEmpty] if not yet sent, or [gochan.ErrClosed] if closed or
// already consumed.
func (rx *Receiver[T]) TryRecv() (T, error) {
	s := rx.s
	select {
	case <-s.done:
		return rx.consume()
	default:
		var z T
		return z, gochan.ErrEmpty
	}
}

// RecvContext blocks until the value is sent or ctx is cancelled. Cancelling
// the context does not close the receiver.
//
// Precedence is closed > cancelled > value, applied by [Receiver.resolve]
// in one pass over the slot: a pair already closed without a pending value
// reports [gochan.ErrClosed] even for a cancelled ctx, since that is the
// durable answer no retry could change, while a cancelled ctx on a live
// pair outranks an already-sent value and leaves it in the slot for a
// later Recv. Once parked, a value and a cancellation landing together
// resolve at random.
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	if v, err, ok := rx.resolve(ctx.Err()); ok {
		return v, err
	}
	select {
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	case <-rx.s.done:
		return rx.consume()
	}
}

// Chan returns a native channel that yields the value once and is then
// closed, or closes empty if the pair is cancelled before a successful
// Send. Useful in a select. Repeated calls return the same channel.
//
// If Chan is used, the value is delivered there and a subsequent Recv on
// the same receiver returns [gochan.ErrClosed] — pick one consumption
// mechanism per receiver.
func (rx *Receiver[T]) Chan() <-chan T {
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userCh != nil {
		return s.userCh
	}
	ch := make(chan T, 1)
	s.userCh = ch
	if s.terminalLocked() {
		// Late registration after a terminal event: deliver inline or close empty.
		if s.hasVal && !s.rxClosed {
			ch <- s.val
			var z T
			s.val = z
			s.hasVal = false
			s.rxClosed = true
		}
		close(ch)
	}
	return ch
}

// Close cancels the channel from the receiver side. Idempotent.
func (rx *Receiver[T]) Close() {
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rxClosed {
		return
	}
	wasTerminal := s.terminalLocked()
	s.rxClosed = true
	if s.hasVal {
		// Drop any sent-but-unconsumed value as documented.
		var z T
		s.val = z
		s.hasVal = false
	}
	if !wasTerminal {
		if s.userCh != nil {
			close(s.userCh)
		}
		close(s.done)
	}
}

// resolve applies the closed > cancelled > value precedence in a single
// critical section, reporting whether it reached an answer.
//
// One acquisition, not a peek followed by a consume: the terminal test and
// the value read have to agree with each other, and splitting them across
// two acquisitions both re-reads the same fields and lets a racing Close
// land in between. ctxErr is passed in rather than consulted here so the
// whole ordering is legible in one place — a cancelled ctx loses to an
// already-terminal pair and beats a waiting value, which it leaves in the
// slot rather than consuming.
//
// ok is false only for a live pair, an empty slot and a live ctx: the one
// state with no answer yet, where the caller must park. That case carries
// [gochan.ErrEmpty] rather than a nil error so a caller that knows the
// pair is terminal can ignore ok without a zero value ever being reported
// as a successful receive.
func (rx *Receiver[T]) resolve(ctxErr error) (T, error, bool) {
	var z T
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.txClosed || s.rxClosed:
		return z, gochan.ErrClosed, true
	case ctxErr != nil:
		return z, ctxErr, true
	case s.hasVal:
		v := s.val
		s.val = z
		s.hasVal = false
		s.rxClosed = true
		return v, nil, true
	}
	return z, gochan.ErrEmpty, false
}

// consume returns the slot value or ErrClosed if unavailable. Callers must
// have observed s.done closed, which makes the pair terminal, so resolve
// always reaches an answer and ok is discarded rather than branched on —
// a branch on it would be unreachable. Should that invariant ever break,
// resolve's undecided case reports ErrEmpty, so the failure would surface
// as an unexpected error rather than as a zero value with a nil error.
func (rx *Receiver[T]) consume() (T, error) {
	v, err, _ := rx.resolve(nil)
	return v, err
}
