package chancore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/internal/parked"
)

// hookedLock wraps a Mutex and runs onLock inside Lock(), letting tests
// mutate fixture state at the exact moment a Send / TrySend / SendContext
// has acquired the lock. Used to force the ChClosed-TOCTOU branch.
type hookedLock struct {
	inner  sync.Mutex
	onLock func()
}

func (h *hookedLock) Lock() {
	h.inner.Lock()
	if h.onLock != nil {
		h.onLock()
	}
}

func (h *hookedLock) Unlock() { h.inner.Unlock() }

// lockSignal reports lock acquisitions to a waiting test without ever
// blocking inside Lock(). The non-blocking send matters: hookedLock runs
// onLock while holding h.inner, so a hook that parked — as a plain
// `ch <- struct{}{}` would once the implementation took the lock more
// times than the test sized the buffer for — would wedge the mutex and
// hang every subsequent Send, reporting a regression as a 10-minute
// timeout and a goroutine dump instead of a failed assertion.
//
// Dropping the surplus signal cannot itself be the detector: whether a
// send lands in a free slot depends on how the test's waits interleave
// with it. count is therefore incremented unconditionally, so assertExact
// sees every acquisition regardless of scheduling.
type lockSignal struct {
	ch    chan struct{}
	count atomic.Int32
}

// newLockSignal sizes the buffer to the number of acquisitions the test
// expects to wait on.
func newLockSignal(want int) *lockSignal {
	return &lockSignal{ch: make(chan struct{}, want)}
}

// hook returns the onLock function to install on a hookedLock.
func (l *lockSignal) hook() func() {
	return func() {
		l.count.Add(1)
		select {
		case l.ch <- struct{}{}:
		default:
		}
	}
}

// wait consumes one acquisition signal, blocking until it arrives.
func (l *lockSignal) wait() { <-l.ch }

// assertExact pins the total number of lock acquisitions. Call once every
// send under test has returned, so the count is settled.
func (l *lockSignal) assertExact(t *testing.T, want int) {
	t.Helper()
	assert.Equal(t, int32(want), l.count.Load(), "unexpected number of SendLock acquisitions")
}

// snapshotSend baselines the Dead-aware select of BufferedSend's fn
// ("Send" or "SendContext"). Take it before spawning the sender, then
// Wait for it to park.
//
// A lockSignal cannot stand in for this. Its hook fires inside Lock(),
// several statements before the closed() recheck and the non-blocking
// probe, so a test that drains the buffer on that signal is racing the
// sender's probe: if the drain lands first the probe succeeds, the
// parking select is never entered, and every assertion still passes — the
// test reports green while covering the fast path it exists to rule out.
//
// The frame spans both selects that can park in these functions: the
// needReady gate and the main send-select. That is not ambiguous for any
// test below, because only one of the two is reachable in each — a
// fixture with a nil (already-latched) Ready skips the gate entirely,
// and one whose Ready never latches can never get past it. Each test
// waits for "parked in Send", which is what its trigger needs.
func snapshotSend(fn string) *parked.Baseline {
	return parked.Snapshot(parked.InSelect, "chancore.(*BufferedSend[...])."+fn+"(")
}

// newSendFixtureWith builds the BufferedSend every fixture below shares,
// wiring lock as both SendLock and CloseLock. That single-mutex shape is
// the fixture's own, not any shipping package's — mpmc, the only
// production consumer, splits the pair across one RWMutex. It is what
// lets a hookedLock stand in for both roles at once.
//
// It is the one place the wiring lives: the named fixtures differ
// only in which handles they hand back, so a change here — the Dead type,
// a new field — cannot land in one test setup and miss another.
//
// A nil ready means "this test has no readiness condition under test".
// BufferedSend.Ready is required, so the fixture substitutes an
// already-latched CloseOnce — the same thing a caller with no gate would
// pass — rather than letting nil reach the struct.
func newSendFixtureWith(capacity int, ready *CloseOnce, lock sync.Locker) (*BufferedSend[int], *CloseOnce, chan int, *atomic.Bool) {
	if ready == nil {
		ready = NewCloseOnce()
		ready.Close()
	}
	dead := NewCloseOnce()
	chClosed := new(atomic.Bool)
	ch := make(chan int, capacity)
	s := &BufferedSend[int]{
		Ch:        ch,
		Dead:      dead,
		Ready:     ready,
		ChClosed:  chClosed,
		SendLock:  lock,
		CloseLock: lock,
	}
	return s, dead, ch, chClosed
}

// newHookedSendFixture is newSendFixtureWith on a hookedLock, handing back
// everything the hooked tests use: the lock to install a callback on, the
// underlying channel to drain a parked Send, and the ChClosed flag. It is
// deliberately one helper rather than one per subset — a variant that
// dropped the fields its callers ignore would refork the setup that
// newSendFixtureWith exists to keep single. Discard what you don't need.
func newHookedSendFixture(capacity int, ready *CloseOnce) (*BufferedSend[int], *CloseOnce, *hookedLock, chan int, *atomic.Bool) {
	lock := &hookedLock{}
	s, dead, ch, chClosed := newSendFixtureWith(capacity, ready, lock)
	return s, dead, lock, ch, chClosed
}

// CloseOnce ---------------------------------------------------------------

func TestCloseOnceCloseReturnsTrueExactlyOnce(t *testing.T) {
	c := NewCloseOnce()
	assert.True(t, c.Close())
	assert.False(t, c.Close())
	assert.False(t, c.Close())
}

func TestCloseOnceIsClosedAndDoneAreConsistent(t *testing.T) {
	c := NewCloseOnce()
	assert.False(t, c.IsClosed())
	select {
	case <-c.Done():
		t.Fatal("Done() should not be closed initially")
	default:
	}

	c.Close()

	assert.True(t, c.IsClosed())
	select {
	case <-c.Done():
	default:
		t.Fatal("Done() should be closed after Close")
	}
}

func TestCloseOnceConcurrentCloseHasOneWinner(t *testing.T) {
	const N = 64
	c := NewCloseOnce()
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			if c.Close() {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int32(1), wins.Load())
	assert.True(t, c.IsClosed())
}

// BufferedSend ------------------------------------------------------------

func newSendFixture(capacity int, ready *CloseOnce) (*BufferedSend[int], *CloseOnce) {
	s, dead, _, _ := newSendFixtureWith(capacity, ready, &sync.Mutex{})
	return s, dead
}

// Stress-counted because the no-priority bug (e.g. removing the pre-select
// on Dead) would let Go's runtime pick the Ch arm ~half the time;
// failing reliably here requires both arms to be ready over many trials.
func TestBufferedSendDeadBeatsBuffer(t *testing.T) {
	const N = 1000
	for i := 0; i < N; i++ {
		s, dead := newSendFixture(1, nil)
		dead.Close()
		err := s.Send(42)
		require.ErrorIs(t, err, gochan.ErrClosed)
	}
}

func TestBufferedSendTrySendReadyVsFull(t *testing.T) {
	ready := NewCloseOnce()
	s, _ := newSendFixture(0, ready)

	// Latch not closed: ErrNotReady regardless of buffer state.
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrNotReady)

	// Latch closed, cap=0, no parked receiver: ErrFull.
	ready.Close()
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrFull)
}

// TestCloseCh covers CloseCh, including the idempotent fast-path.
func TestCloseCh(t *testing.T) {
	s, _, ch, chClosed := newSendFixtureWith(1, nil, &sync.Mutex{})
	s.CloseCh()
	s.CloseCh() // idempotent
	_, ok := <-ch
	assert.False(t, ok)
	assert.True(t, chClosed.Load())
}

// TestBufferedSendDeadInNeedReady covers Send's "<-s.Dead" arm inside the
// needReady select — Dead fires while Send is parked waiting for the
// readiness latch to close.
func TestBufferedSendDeadInNeedReady(t *testing.T) {
	ready := NewCloseOnce() // never latched
	s, dead := newSendFixture(1, ready)
	errCh := make(chan error, 1)
	base := snapshotSend("Send")
	go func() { errCh <- s.Send(1) }()
	base.Wait(t, 1)
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedSendChClosedAfterLock covers Send's post-SendLock ChClosed
// TOCTOU branch via a hookedLock that flips ChClosed inside Lock().
func TestBufferedSendChClosedAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, _, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.Send(1), gochan.ErrClosed)
}

// TestBufferedSendDeadDuringParkedSend covers Send's "<-s.Dead" arm in the
// main send-select — Send parked on a full / rendezvous channel, then Dead
// fires.
func TestBufferedSendDeadDuringParkedSend(t *testing.T) {
	s, dead := newSendFixture(0, nil) // rendezvous, no receiver
	errCh := make(chan error, 1)
	base := snapshotSend("Send")
	go func() { errCh <- s.Send(1) }()
	base.Wait(t, 1)
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedTrySendDeadFirstArm covers TrySend's first-select Dead arm.
func TestBufferedTrySendDeadFirstArm(t *testing.T) {
	s, dead := newSendFixture(1, nil)
	dead.Close()
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrClosed)
}

// TestBufferedTrySendChClosedAfterLock covers TrySend's post-lock ChClosed
// TOCTOU branch.
func TestBufferedTrySendChClosedAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, _, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrClosed)
}

// TrySend's main send-select no longer carries a <-Dead arm: the atomic
// Dead recheck above it decides that outcome deterministically, so the
// old randomised, non-asserting coverage test for that arm is superseded
// by TestBufferedTrySendDeadAfterLockBeatsBufferSpace.

// TestBufferedSendContextDeadFirstArm covers SendContext's first-select
// Dead arm.
func TestBufferedSendContextDeadFirstArm(t *testing.T) {
	s, dead := newSendFixture(1, nil)
	dead.Close()
	assert.ErrorIs(t, s.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestBufferedSendContextDeadInNeedReady covers SendContext's Dead arm in
// the needReady select.
func TestBufferedSendContextDeadInNeedReady(t *testing.T) {
	ready := NewCloseOnce() // never latched
	s, dead := newSendFixture(1, ready)
	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(context.Background(), 1) }()
	base.Wait(t, 1)
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedSendContextChClosedAfterLock covers SendContext's post-lock
// ChClosed TOCTOU branch.
func TestBufferedSendContextChClosedAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, _, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestBufferedSendContextDeadDuringParkedSend covers SendContext's Dead
// arm in the main send-select.
func TestBufferedSendContextDeadDuringParkedSend(t *testing.T) {
	s, dead := newSendFixture(0, nil)
	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(context.Background(), 1) }()
	base.Wait(t, 1)
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedSendContextCtxAlreadyCancelled covers SendContext's
// ctx.Err()-already-set early return.
func TestBufferedSendContextCtxAlreadyCancelled(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, s.SendContext(ctx, 1), context.Canceled)
}

// TestBufferedSendContextClosedBeatsCancelled pins SendContext's entry
// precedence: a termination visible on entry outranks an already-cancelled
// ctx, and both arms of closed() rank the same way. Dead used to sit below
// the ctx check while ChClosed sat above it, so the same logical condition
// reported two different errors depending on which arm fired.
func TestBufferedSendContextClosedBeatsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Dead", func(t *testing.T) {
		s, dead := newSendFixture(1, nil)
		dead.Close()
		assert.ErrorIs(t, s.SendContext(ctx, 1), gochan.ErrClosed)
	})
	t.Run("ChClosed", func(t *testing.T) {
		s, _, _, chClosed := newSendFixtureWith(1, nil, &sync.Mutex{})
		chClosed.Store(true)
		assert.ErrorIs(t, s.SendContext(ctx, 1), gochan.ErrClosed)
	})
	// A live sender still reports the caller's cancellation.
	t.Run("Live", func(t *testing.T) {
		s, _ := newSendFixture(1, nil)
		assert.ErrorIs(t, s.SendContext(ctx, 1), context.Canceled)
	})
}

// TestBufferedSendChClosedPreCheck covers Send's first ChClosed.Load()
// branch — channel already marked closed before Send runs.
func TestBufferedSendChClosedPreCheck(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	s.CloseCh()
	assert.ErrorIs(t, s.Send(1), gochan.ErrClosed)
}

// TestBufferedSendHappyPath covers Send's "case s.Ch <- v: return nil"
// arm in the main send-select.
func TestBufferedSendHappyPath(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	require.NoError(t, s.Send(7))
}

// TestBufferedSendReadyLatchesDuringWait covers Send's
// "case <-s.Ready.Done()" arm in the needReady select — Ready closes
// while Send is parked.
func TestBufferedSendReadyLatchesDuringWait(t *testing.T) {
	ready := NewCloseOnce()
	s, _ := newSendFixture(1, ready)
	errCh := make(chan error, 1)
	base := snapshotSend("Send")
	go func() { errCh <- s.Send(1) }()
	base.Wait(t, 1)
	ready.Close()
	require.NoError(t, <-errCh)
}

// TestBufferedTrySendChClosedPreCheck covers TrySend's first ChClosed
// branch.
func TestBufferedTrySendChClosedPreCheck(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	s.CloseCh()
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrClosed)
}

// TestBufferedTrySendHappyPath covers TrySend's "case s.Ch <- v" arm.
func TestBufferedTrySendHappyPath(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	require.NoError(t, s.TrySend(99))
}

// TestBufferedSendContextChClosedPreCheck covers SendContext's first
// ChClosed branch.
func TestBufferedSendContextChClosedPreCheck(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	s.CloseCh()
	assert.ErrorIs(t, s.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestBufferedSendContextHappyPath covers SendContext's
// "case s.Ch <- v: return nil" arm.
func TestBufferedSendContextHappyPath(t *testing.T) {
	s, _ := newSendFixture(1, nil)
	require.NoError(t, s.SendContext(context.Background(), 11))
}

// TestBufferedSendContextCtxCancelledAfterLock pins the post-lock ctx
// recheck: a cancellation landing during the needReady gate or the
// SendLock wait — both unbounded — must be reported, not overtaken by the
// probe finding buffer space. Without the recheck a producer that
// cancelled and moved on still deposits a value and is told it
// succeeded. The hooked lock cancels inside Lock(), which is exactly the
// window that cannot be hit reliably from outside.
func TestBufferedSendContextCtxCancelledAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, ch, _ := newHookedSendFixture(1, ready) // buffer has room
	ctx, cancel := context.WithCancel(context.Background())
	lock.onLock = func() { cancel() }

	assert.ErrorIs(t, s.SendContext(ctx, 42), context.Canceled)
	assert.Empty(t, ch, "cancelled send must not deposit")
}

// TestBufferedSendContextCtxCancelledDuringWait covers SendContext's
// "case <-ctx.Done()" arm in the main send-select.
func TestBufferedSendContextCtxCancelledDuringWait(t *testing.T) {
	s, _ := newSendFixture(0, nil) // rendezvous, no receiver
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(ctx, 1) }()
	base.Wait(t, 1)
	cancel()
	assert.ErrorIs(t, <-errCh, context.Canceled)
}

// TestBufferedSendContextCtxCancelledDuringNeedReady covers SendContext's
// "case <-ctx.Done()" arm in the needReady select.
func TestBufferedSendContextCtxCancelledDuringNeedReady(t *testing.T) {
	ready := NewCloseOnce() // never latched
	s, _ := newSendFixture(1, ready)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(ctx, 1) }()
	base.Wait(t, 1)
	cancel()
	assert.ErrorIs(t, <-errCh, context.Canceled)
}

// TestBufferedSendReadyLatchesAlreadyClosed covers Send's
// "case <-s.Ready.Done()" arm when Ready is already closed at the
// moment the select runs — Go picks Ready.Done over Dead since Dead is
// open.
func TestBufferedSendReadyLatchesAlreadyClosed(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _ := newSendFixture(1, ready)
	require.NoError(t, s.Send(1))
}

// Send fast-path guards ---------------------------------------------------
//
// Send and SendContext probe the value channel with a non-blocking select
// before falling back to the Dead-aware one. The tests below pin the two
// invariants that probe must not break: it runs *after* the Ready gate,
// and a probe miss still parks on the Dead-aware select rather than
// reporting failure.

// Stress-counted for the same reason as TestBufferedSendDeadBeatsBuffer:
// the detector is sound but timing-dependent. If the send-select probe
// were hoisted above the needReady gate, Send would take SendLock while
// Ready is still unlatched — the onLock hook catches exactly that.
//
// Waiting for the send to park replaces the repetition this used to rely
// on: ready.Close() now lands strictly after the goroutine has parked, so
// there is no unlucky run where Ready latches before Send is reached. The
// two detectors catch different regressions. Hoisting the probe above the
// gate means the send never parks at all, which the wait reports; taking
// SendLock before the gate means it parks with the lock held, which
// jumpedGate reports.
func TestBufferedSendWaitsForReadyDespiteBufferSpace(t *testing.T) {
	var jumpedGate atomic.Bool
	ready := NewCloseOnce()
	s, _, lock, _, _ := newHookedSendFixture(1, ready) // buffer has room
	lock.onLock = func() {
		if !ready.IsClosed() {
			jumpedGate.Store(true)
		}
	}
	errCh := make(chan error, 1)
	base := snapshotSend("Send")
	go func() { errCh <- s.Send(1) }()
	base.Wait(t, 1) // parked in the needReady gate, not past it
	ready.Close()
	require.NoError(t, <-errCh)
	assert.False(t, jumpedGate.Load(),
		"Send acquired SendLock while Ready was unlatched: the send-select "+
			"probe must run after the needReady gate, not before it")
}

// TestBufferedSendContextWaitsForReadyDespiteBufferSpace is the
// SendContext twin of TestBufferedSendWaitsForReadyDespiteBufferSpace.
func TestBufferedSendContextWaitsForReadyDespiteBufferSpace(t *testing.T) {
	var jumpedGate atomic.Bool
	ready := NewCloseOnce()
	s, _, lock, _, _ := newHookedSendFixture(1, ready)
	lock.onLock = func() {
		if !ready.IsClosed() {
			jumpedGate.Store(true)
		}
	}
	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(context.Background(), 1) }()
	base.Wait(t, 1) // parked in the needReady gate, not past it
	ready.Close()
	require.NoError(t, <-errCh)
	assert.False(t, jumpedGate.Load(),
		"SendContext acquired SendLock while Ready was unlatched: the "+
			"send-select probe must run after the needReady gate")
}

// Dead-after-lock guards --------------------------------------------------
//
// The entry-time Dead pre-check runs before the needReady gate and before
// SendLock is acquired, either of which can block for an unbounded time.
// These tests pin the behaviour when Dead closes inside that window with
// buffer space available: the send must report ErrClosed rather than
// depositing into a queue no receiver will drain. Same hookedLock shape
// as TestBufferedSendChClosedAfterLock.

func TestBufferedSendDeadAfterLockBeatsBufferSpace(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, dead, lock, _, _ := newHookedSendFixture(1, ready) // buffer has room
	lock.onLock = func() { dead.Close() }
	assert.ErrorIs(t, s.Send(1), gochan.ErrClosed)
}

func TestBufferedTrySendDeadAfterLockBeatsBufferSpace(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, dead, lock, _, _ := newHookedSendFixture(1, ready)
	lock.onLock = func() { dead.Close() }
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrClosed)
}

func TestBufferedSendContextDeadAfterLockBeatsBufferSpace(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, dead, lock, _, _ := newHookedSendFixture(1, ready)
	lock.onLock = func() { dead.Close() }
	assert.ErrorIs(t, s.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestBufferedSendFullBufferParksThenCompletes pins the probe-miss arm:
// with the buffer full the non-blocking probe cannot succeed, so Send
// must park on the Dead-aware select and complete once a receive makes
// room — not return ErrFull or drop the value.
//
// The drain must not happen until the second Send is actually parked —
// see snapshotSend for why the lock signal cannot establish that. The
// lockSignal is still installed to pin the acquisition count.
func TestBufferedSendFullBufferParksThenCompletes(t *testing.T) {
	s, _, lock, ch, _ := newHookedSendFixture(1, nil)
	sig := newLockSignal(2) // one acquisition per Send
	lock.onLock = sig.hook()

	require.NoError(t, s.Send(1)) // fills the buffer
	sig.wait()                    // consume the first Send's signal

	errCh := make(chan error, 1)
	base := snapshotSend("Send")
	go func() { errCh <- s.Send(2) }()
	base.Wait(t, 1) // probe has missed; committed to the select

	assert.Equal(t, 1, <-ch) // makes room, unparking the second Send
	require.NoError(t, <-errCh)
	assert.Equal(t, 2, <-ch)
	sig.assertExact(t, 2)
}

// TestBufferedSendContextFullBufferParksThenCompletes is the SendContext
// twin of TestBufferedSendFullBufferParksThenCompletes.
func TestBufferedSendContextFullBufferParksThenCompletes(t *testing.T) {
	s, _, lock, ch, _ := newHookedSendFixture(1, nil)
	sig := newLockSignal(2)
	lock.onLock = sig.hook()

	require.NoError(t, s.SendContext(context.Background(), 1))
	sig.wait()

	errCh := make(chan error, 1)
	base := snapshotSend("SendContext")
	go func() { errCh <- s.SendContext(context.Background(), 2) }()
	base.Wait(t, 1)

	assert.Equal(t, 1, <-ch)
	require.NoError(t, <-errCh)
	assert.Equal(t, 2, <-ch)
	sig.assertExact(t, 2)
}
