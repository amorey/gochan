// Package oneshot provides a single-value, single-delivery channel.
//
// Exactly one Send will ever succeed and the value is delivered to exactly
// one Recv. Either side may cancel by closing its handle; the other side
// observes [gochan.ErrClosed] on its next operation.
package oneshot

import (
	"context"
	"sync"

	"github.com/yourname/gochan"
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

// New creates a fresh oneshot pair sharing a single value slot.
func New[T any]() (*Sender[T], *Receiver[T]) {
	s := &shared[T]{done: make(chan struct{})}
	return &Sender[T]{s: s}, &Receiver[T]{s: s}
}

// Send deposits v into the slot and returns immediately without waiting for a
// receiver. Returns [gochan.ErrClosed] if the pair is already terminated.
func (tx *Sender[T]) Send(v T) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalLocked() {
		return gochan.ErrClosed
	}
	s.val = v
	s.hasVal = true
	if s.userCh != nil {
		// Chan() was registered earlier; deliver inline (cap-1, never pushed).
		s.userCh <- v
		close(s.userCh)
		var z T
		s.val = z
		s.hasVal = false
		s.rxClosed = true
	}
	close(s.done)
	return nil
}

// TrySend is equivalent to Send: oneshot Send never blocks.
func (tx *Sender[T]) TrySend(v T) error { return tx.Send(v) }

// SendContext returns ctx.Err() if ctx is already cancelled; otherwise it
// behaves like Send. Send never blocks, so there is nothing for cancellation
// to interrupt mid-call.
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error {
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
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error) {
	s := rx.s
	// Prefer a ready value over a cancelled context when both are available.
	select {
	case <-s.done:
		return rx.consume()
	default:
	}
	select {
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	case <-s.done:
		return rx.consume()
	}
}

// Chan returns a native channel that yields the value once and is then
// closed. Repeated calls return the same channel.
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

// consume returns the slot value or ErrClosed if unavailable. Caller must
// have observed s.done closed (or be the only path that knows it's safe).
func (rx *Receiver[T]) consume() (T, error) {
	s := rx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasVal || s.rxClosed {
		var z T
		return z, gochan.ErrClosed
	}
	v := s.val
	var z T
	s.val = z
	s.hasVal = false
	s.rxClosed = true
	return v, nil
}
