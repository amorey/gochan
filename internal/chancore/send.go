package chancore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/amorey/gochan"
)

// BufferedSend is the shared send-side core for chan-backed pipelines. A
// successful Send writes to Ch under SendLock; Dead is the termination
// signal that lets an in-flight send escape close(Ch). Ready gates the
// send-select on a one-shot readiness condition (e.g. "at least one
// receiver has registered"); once Ready has latched, senders skip the
// Ready arm via the atomic IsClosed fast-path rather than carrying a
// permanently-ready channel into every select.
//
// ChClosed is an atomic fast-path flag flipped to true by the close path
// just before close(Ch). Senders check it before and after acquiring
// SendLock so a closed channel is never written to; see [closed] for why
// the recheck is load-bearing.
//
// Dead and Ready are both required, and are *CloseOnce rather than bare
// channels so senders can re-test them with an atomic load after taking
// SendLock. A nil Dead would not mean "never terminates" — Dead is the
// only arm that unparks a sender blocked on a full buffer at tear-down,
// so nil would leak those goroutines. A send-side with no readiness
// condition passes an already-latched CloseOnce instead of nil, which
// keeps one code path here rather than two.
//
// SendLock and CloseLock are the reader and writer halves of the
// exclusion that keeps [CloseCh] from closing Ch out from under an
// in-flight sender; mpmc, the only production consumer, wires both to one
// RWMutex (SendLock is chMu.RLocker(), CloseLock is &chMu). They are
// sync.Locker rather than a concrete *sync.RWMutex, and SendLock ==
// CloseLock is a supported wiring, because the tests inject a lock whose
// Lock() runs a callback — the only way to drive the closed()-after-lock
// and Dead-after-lock branches.
type BufferedSend[T any] struct {
	Ch        chan<- T
	Dead      *CloseOnce // required, never nil
	Ready     *CloseOnce // required, never nil; latch it if there is no gate
	ChClosed  *atomic.Bool
	SendLock  sync.Locker
	CloseLock sync.Locker
}

// needReady reports whether the readiness arm still needs to be evaluated
// on the slow path. Once Ready has latched this is false forever, so the
// gate below it compiles down to one atomic load on the steady-state hot
// path.
func (s *BufferedSend[T]) needReady() bool {
	return !s.Ready.IsClosed()
}

// closed reports whether this send-side is terminated by either arm:
// ChClosed (Ch is closed, or is about to be) or Dead (the pipeline is torn
// down). Both mean [gochan.ErrClosed] to a caller, so they are always
// tested together and in that order — ChClosed is the arm that makes a
// write to Ch unsafe, so it must not be gated behind anything.
//
// Every send path calls this twice: once on entry, and again after taking
// SendLock. The recheck is load-bearing rather than defensive. The
// needReady gate and the lock can each block for an unbounded time, so the
// entry-time answer may be stale by the moment a value would actually be
// deposited; without it a send that lost that race would enqueue into a
// queue no receiver will drain and report success. Two atomic loads
// (~1ns), cheap enough to sit on the hot path.
func (s *BufferedSend[T]) closed() bool {
	return s.ChClosed.Load() || s.Dead.IsClosed()
}

// CloseCh marks Ch as closed and closes it under CloseLock. Idempotent.
func (s *BufferedSend[T]) CloseCh() {
	s.CloseLock.Lock()
	defer s.CloseLock.Unlock()
	if s.ChClosed.CompareAndSwap(false, true) {
		close(s.Ch)
	}
}

// Send enqueues v, blocking while the buffer is full and, until Ready has
// latched, while the readiness condition is unmet. Returns
// [gochan.ErrClosed] if the send-side is terminated on entry, becomes
// terminated while waiting on Ready, or is found terminated after
// SendLock is acquired.
func (s *BufferedSend[T]) Send(v T) error {
	if s.closed() {
		return gochan.ErrClosed
	}
	if s.needReady() {
		select {
		case <-s.Dead.Done():
			return gochan.ErrClosed
		case <-s.Ready.Done():
		}
	}
	s.SendLock.Lock()
	defer s.SendLock.Unlock()
	if s.closed() {
		return gochan.ErrClosed
	}
	// Probe non-blockingly first: a single-case select with default
	// compiles to selectnbsend, roughly half the cost of the two-case
	// selectgo below, and the buffer usually has room. It stays below the
	// needReady gate and the closed() recheck because it optimizes the
	// send, not the guards.
	//
	// It also resolves, in the buffer's favour, the post-recheck window
	// TrySend documents below: a Dead closing after the recheck yields a
	// successful send into a queue no receiver will drain. The window
	// cannot be closed — that would take firing Dead under CloseLock, but
	// a sender parked on the select below holds SendLock and only wakes
	// when Dead closes, so the closer would wait on the very sender
	// waiting on it. (That is why mpmc keeps dead outside chMu.) Given
	// that, the only choice is how to resolve it, and a deterministic
	// "buffer had room → accepted" beats the bare select's coin flip: it
	// is what a plain buffered channel does when its last reader goes
	// away.
	//
	// The Dead arm stays in the select below regardless: with the buffer
	// full it is the only thing that unparks a sender at tear-down.
	select {
	case s.Ch <- v:
		return nil
	default:
	}
	select {
	case s.Ch <- v:
		return nil
	case <-s.Dead.Done():
		return gochan.ErrClosed
	}
}

// TrySend is the non-blocking variant of Send.
func (s *BufferedSend[T]) TrySend(v T) error {
	if s.closed() {
		return gochan.ErrClosed
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
	if s.closed() {
		return gochan.ErrClosed
	}
	// The recheck above is the only Dead test needed: TrySend never parks,
	// so the select below stays single-case-with-default (selectnbsend).
	// A Dead closing after it surfaces as ErrFull or as a successful send
	// rather than ErrClosed, which is accepted — the next TrySend hits the
	// entry check, so a retry loop pays at most one extra backoff. Adding
	// a <-Dead arm would resolve Dead-vs-buffer-space at random instead,
	// breaking TestBufferedTrySendDeadAfterLockBeatsBufferSpace.
	select {
	case s.Ch <- v:
		return nil
	default:
		return gochan.ErrFull
	}
}

// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled
// before the value is enqueued.
//
// A termination already visible on entry outranks a cancelled ctx: the
// call returns [gochan.ErrClosed] even if ctx was cancelled first. Once
// the call is parked the two are select arms and a close landing together
// with a cancellation resolves at random, so callers should treat either
// error as terminal for this send rather than depending on which arrives.
func (s *BufferedSend[T]) SendContext(ctx context.Context, v T) error {
	// Termination outranks cancellation: ErrClosed is durable and tells the
	// caller this sender is finished, whereas ctx.Err() invites a retry
	// with a fresh context that would only return ErrClosed anyway. Testing
	// closed() as a unit also keeps the two arms from disagreeing — Dead
	// used to sit below the ctx check while ChClosed sat above it, so the
	// same logical condition had two different precedences depending on
	// which arm reported it. Pinned by
	// TestBufferedSendContextClosedBeatsCancelled.
	if s.closed() {
		return gochan.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.needReady() {
		select {
		case <-s.Dead.Done():
			return gochan.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-s.Ready.Done():
		}
	}
	s.SendLock.Lock()
	defer s.SendLock.Unlock()
	if s.closed() {
		return gochan.ErrClosed
	}
	// Recheck ctx for the same reason closed() is rechecked above, and in
	// the same order: the needReady gate and the lock each block for an
	// unbounded time, so the entry-time answer can be stale by the moment
	// a value would actually be deposited. Without this, a ctx cancelled
	// during either wait is never consulted again — the probe below finds
	// room and deposits, so a producer that cancelled and moved on still
	// enqueues a value and is told it succeeded.
	//
	// This is where cancellation differs from Dead, whose post-recheck
	// window the probe deliberately resolves in the buffer's favour. That
	// window cannot be closed (firing Dead under CloseLock deadlocks, see
	// Send), and ErrClosed there would only invite a retry the entry check
	// refuses anyway. A cancellation has neither excuse: it is cheap to
	// re-read, and the caller has withdrawn rather than been shut down.
	// Matches the receive side, where a cancelled ctx outranks a ready
	// value. Pinned by TestBufferedSendContextCtxCancelledAfterLock.
	if err := ctx.Err(); err != nil {
		return err
	}
	// Same probe, same accepted Dead window, as Send.
	select {
	case s.Ch <- v:
		return nil
	default:
	}
	select {
	case s.Ch <- v:
		return nil
	case <-s.Dead.Done():
		return gochan.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
