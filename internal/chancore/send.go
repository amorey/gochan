package chancore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
)

// BufferedSend is the shared send-side core for chan-backed pipelines. A
// successful Send writes to Ch under SendLock; Dead is the termination
// signal that lets an in-flight send escape close(Ch). Ready, if non-nil,
// gates the send-select on a one-shot readiness condition (e.g. "at least
// one receiver has registered"); once Ready has latched, senders skip the
// Ready arm via the atomic IsClosed fast-path rather than carrying a
// permanently-ready channel into every select.
//
// ChClosed is an atomic fast-path flag flipped to true by the close path
// just before close(Ch). Senders check it before and after acquiring
// SendLock so a closed channel is never written to.
type BufferedSend[T any] struct {
	Ch       chan<- T
	Dead     <-chan struct{}
	Ready    *CloseOnce // nil = no readiness gate
	ChClosed *atomic.Bool
	SendLock sync.Locker
}

// needReady reports whether the readiness arm still needs to be evaluated
// on the slow path.
func (s *BufferedSend[T]) needReady() bool {
	return s.Ready != nil && !s.Ready.IsClosed()
}

// CloseCh marks Ch as closed and closes it. closeLock is the writer-side
// lock that excludes in-flight senders (usually the same lock SendLock
// wraps, or its write-side for RWMutex setups). Idempotent.
func (s *BufferedSend[T]) CloseCh(closeLock sync.Locker) {
	closeLock.Lock()
	defer closeLock.Unlock()
	if s.ChClosed.CompareAndSwap(false, true) {
		close(s.Ch)
	}
}

// Send enqueues v, blocking while the buffer is full and (if Ready != nil)
// while no consumer is registered.
func (s *BufferedSend[T]) Send(v T) error {
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.Dead:
		return gochan.ErrClosed
	default:
	}
	if s.needReady() {
		select {
		case <-s.Dead:
			return gochan.ErrClosed
		case <-s.Ready.Done():
		}
	}
	s.SendLock.Lock()
	defer s.SendLock.Unlock()
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.Ch <- v:
		return nil
	case <-s.Dead:
		return gochan.ErrClosed
	}
}

// TrySend is the non-blocking variant of Send.
func (s *BufferedSend[T]) TrySend(v T) error {
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case <-s.Dead:
		return gochan.ErrClosed
	default:
	}
	if s.needReady() {
		select {
		case <-s.Ready.Done():
		default:
			return gochan.ErrNotReady
		}
	}
	s.SendLock.Lock()
	defer s.SendLock.Unlock()
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.Ch <- v:
		return nil
	case <-s.Dead:
		return gochan.ErrClosed
	default:
		return gochan.ErrFull
	}
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
func (s *BufferedSend[T]) SendContext(ctx context.Context, v T) error {
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.Dead:
		return gochan.ErrClosed
	default:
	}
	if s.needReady() {
		select {
		case <-s.Dead:
			return gochan.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-s.Ready.Done():
		}
	}
	s.SendLock.Lock()
	defer s.SendLock.Unlock()
	if s.ChClosed.Load() {
		return gochan.ErrClosed
	}
	select {
	case s.Ch <- v:
		return nil
	case <-s.Dead:
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
