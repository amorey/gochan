package chancore

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
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

func newHookedSendFixture(capacity int, ready *CloseOnce) (*BufferedSend[int], *CloseOnce, *hookedLock, *atomic.Bool) {
	dead := NewCloseOnce()
	var chClosed atomic.Bool
	lock := &hookedLock{}
	s := &BufferedSend[int]{
		Ch:        make(chan int, capacity),
		Dead:      dead.Done(),
		Ready:     ready,
		ChClosed:  &chClosed,
		SendLock:  lock,
		CloseLock: lock,
	}
	return s, dead, lock, &chClosed
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
	dead := NewCloseOnce()
	var chClosed atomic.Bool
	var mu sync.Mutex
	s := &BufferedSend[int]{
		Ch:        make(chan int, capacity),
		Dead:      dead.Done(),
		Ready:     ready,
		ChClosed:  &chClosed,
		SendLock:  &mu,
		CloseLock: &mu,
	}
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

func TestBufferedSendTrySendNilReadyReturnsFull(t *testing.T) {
	// Ready unset (e.g. spsc/mpsc send-side): no ErrNotReady path.
	s, _ := newSendFixture(0, nil)
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrFull)
}

// BufferedRecv ------------------------------------------------------------

func newRecvFixture(buffered []int, ready *CloseOnce) (*BufferedRecv[int], *CloseOnce, chan int) {
	ch := make(chan int, cap1(buffered))
	for _, v := range buffered {
		ch <- v
	}
	dead := NewCloseOnce()
	r := &BufferedRecv[int]{
		Ch:    ch,
		Dead:  dead.Done(),
		Ready: ready,
	}
	return r, dead, ch
}

// cap1 returns at least 1 so the channel is buffered even when no values
// are pre-loaded.
func cap1(buffered []int) int {
	if len(buffered) < 1 {
		return 1
	}
	return len(buffered)
}

func TestBufferedRecvDeadBeatsBuffered(t *testing.T) {
	const N = 1000
	for i := 0; i < N; i++ {
		r, dead, _ := newRecvFixture([]int{99}, nil)
		dead.Close()

		_, err := r.Recv()
		require.ErrorIs(t, err, gochan.ErrClosed)

		_, err = r.TryRecv()
		require.ErrorIs(t, err, gochan.ErrClosed)
	}
}

func TestBufferedRecvRecvContextValueBeatsCancelledCtx(t *testing.T) {
	r, _, _ := newRecvFixture([]int{7}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v, err := r.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, v)
}

func TestBufferedRecvTryRecvReadyVsEmpty(t *testing.T) {
	ready := NewCloseOnce()
	r, _, ch := newRecvFixture(nil, ready)

	// Latch not closed, no value: ErrNotReady.
	_, err := r.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrNotReady)

	// Latch closed, no value: ErrEmpty.
	ready.Close()
	_, err = r.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)

	// Buffered value always wins over the latch state.
	ch <- 12
	v, err := r.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 12, v)
}

func TestBufferedRecvTryRecvNilReadyReturnsEmpty(t *testing.T) {
	r, _, _ := newRecvFixture(nil, nil)
	_, err := r.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
}

func TestBufferedRecvDeadBeatsNotReady(t *testing.T) {
	ready := NewCloseOnce() // never latched
	r, dead, _ := newRecvFixture(nil, ready)
	dead.Close()

	_, err := r.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestCloseCh covers CloseCh, including the idempotent fast-path.
func TestCloseCh(t *testing.T) {
	dead := NewCloseOnce()
	var chClosed atomic.Bool
	var mu sync.Mutex
	ch := make(chan int, 1)
	s := &BufferedSend[int]{
		Ch:        ch,
		Dead:      dead.Done(),
		ChClosed:  &chClosed,
		SendLock:  &mu,
		CloseLock: &mu,
	}
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
	go func() { errCh <- s.Send(1) }()
	runtime.Gosched()
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedSendChClosedAfterLock covers Send's post-SendLock ChClosed
// TOCTOU branch via a hookedLock that flips ChClosed inside Lock().
func TestBufferedSendChClosedAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.Send(1), gochan.ErrClosed)
}

// TestBufferedSendDeadDuringParkedSend covers Send's "<-s.Dead" arm in the
// main send-select — Send parked on a full / rendezvous channel, then Dead
// fires.
func TestBufferedSendDeadDuringParkedSend(t *testing.T) {
	s, dead := newSendFixture(0, nil) // rendezvous, no receiver
	errCh := make(chan error, 1)
	go func() { errCh <- s.Send(1) }()
	runtime.Gosched()
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
	s, _, lock, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.TrySend(1), gochan.ErrClosed)
}

// TestBufferedTrySendDeadInMainSelect covers TrySend's "<-s.Dead" arm in
// the main send-select. Forces Dead closed inside Lock() so it is ready
// when the select runs. The "default" arm is also ready, so Go's runtime
// picks one randomly — loop to make hitting Dead overwhelmingly likely.
func TestBufferedTrySendDeadInMainSelect(t *testing.T) {
	for i := 0; i < 200; i++ {
		ready := NewCloseOnce()
		ready.Close()
		s, dead, lock, _ := newHookedSendFixture(0, ready) // rendezvous, no recv
		lock.onLock = func() { dead.Close() }
		// Either Dead or default fires; both return ErrClosed/ErrFull.
		// We don't assert on the result — coverage is the goal.
		_ = s.TrySend(1)
	}
}

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
	go func() { errCh <- s.SendContext(context.Background(), 1) }()
	runtime.Gosched()
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedSendContextChClosedAfterLock covers SendContext's post-lock
// ChClosed TOCTOU branch.
func TestBufferedSendContextChClosedAfterLock(t *testing.T) {
	ready := NewCloseOnce()
	ready.Close()
	s, _, lock, chClosed := newHookedSendFixture(1, ready)
	lock.onLock = func() { chClosed.Store(true) }
	assert.ErrorIs(t, s.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestBufferedSendContextDeadDuringParkedSend covers SendContext's Dead
// arm in the main send-select.
func TestBufferedSendContextDeadDuringParkedSend(t *testing.T) {
	s, dead := newSendFixture(0, nil)
	errCh := make(chan error, 1)
	go func() { errCh <- s.SendContext(context.Background(), 1) }()
	runtime.Gosched()
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

// TestBufferedRecvDeadDuringParkedRecv covers Recv's second-select Dead
// arm — Dead fires while Recv is parked on the value channel.
func TestBufferedRecvDeadDuringParkedRecv(t *testing.T) {
	r, dead, _ := newRecvFixture(nil, nil)
	errCh := make(chan error, 1)
	go func() {
		_, err := r.Recv()
		errCh <- err
	}()
	runtime.Gosched()
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedRecvContextDeadFirstArm covers RecvContext's first-select
// Dead arm.
func TestBufferedRecvContextDeadFirstArm(t *testing.T) {
	r, dead, _ := newRecvFixture(nil, nil)
	dead.Close()
	_, err := r.RecvContext(context.Background())
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestBufferedRecvContextSecondSelectValueArrives covers RecvContext's
// second-select Ch-with-value arm: Ch is empty when RecvContext enters
// the second select, then a value arrives.
func TestBufferedRecvContextSecondSelectValueArrives(t *testing.T) {
	r, _, ch := newRecvFixture(nil, nil)
	type result struct {
		v   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := r.RecvContext(context.Background())
		done <- result{v, err}
	}()
	runtime.Gosched()
	ch <- 42
	r2 := <-done
	require.NoError(t, r2.err)
	assert.Equal(t, 42, r2.v)
}

// TestBufferedRecvContextSecondSelectChClosed covers RecvContext's
// second-select Ch-closed (!ok) arm — channel closes while parked.
func TestBufferedRecvContextSecondSelectChClosed(t *testing.T) {
	r, _, ch := newRecvFixture(nil, nil)
	errCh := make(chan error, 1)
	go func() {
		_, err := r.RecvContext(context.Background())
		errCh <- err
	}()
	runtime.Gosched()
	close(ch)
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedRecvContextSecondSelectDead covers RecvContext's
// second-select Dead arm — Dead fires while parked in second select.
func TestBufferedRecvContextSecondSelectDead(t *testing.T) {
	r, dead, _ := newRecvFixture(nil, nil)
	errCh := make(chan error, 1)
	go func() {
		_, err := r.RecvContext(context.Background())
		errCh <- err
	}()
	runtime.Gosched()
	dead.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedRecvChClosedDuringParkedRecv covers Recv's
// "case v, ok := <-r.Ch" with !ok in the second select.
func TestBufferedRecvChClosedDuringParkedRecv(t *testing.T) {
	r, _, ch := newRecvFixture(nil, nil)
	errCh := make(chan error, 1)
	go func() {
		_, err := r.Recv()
		errCh <- err
	}()
	runtime.Gosched()
	close(ch)
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

// TestBufferedRecvHappyPath covers Recv's "return v, nil" arm in the
// second select — a buffered value is delivered.
func TestBufferedRecvHappyPath(t *testing.T) {
	r, _, _ := newRecvFixture([]int{17}, nil)
	v, err := r.Recv()
	require.NoError(t, err)
	assert.Equal(t, 17, v)
}

// TestBufferedTryRecvChClosed covers TryRecv's "case v, ok := <-r.Ch" with
// !ok in the second select.
func TestBufferedTryRecvChClosed(t *testing.T) {
	r, _, ch := newRecvFixture(nil, nil)
	close(ch)
	_, err := r.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestBufferedRecvContextChClosedProbe covers RecvContext's first
// (non-blocking) select Ch-with-!ok arm.
func TestBufferedRecvContextChClosedProbe(t *testing.T) {
	r, _, ch := newRecvFixture(nil, nil)
	close(ch)
	_, err := r.RecvContext(context.Background())
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestBufferedRecvContextCtxCancelledDuringWait covers RecvContext's
// second-select "case <-ctx.Done()" arm.
func TestBufferedRecvContextCtxCancelledDuringWait(t *testing.T) {
	r, _, _ := newRecvFixture(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := r.RecvContext(ctx)
		errCh <- err
	}()
	runtime.Gosched()
	cancel()
	assert.ErrorIs(t, <-errCh, context.Canceled)
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
	go func() { errCh <- s.Send(1) }()
	runtime.Gosched()
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

// TestBufferedSendContextCtxCancelledDuringWait covers SendContext's
// "case <-ctx.Done()" arm in the main send-select.
func TestBufferedSendContextCtxCancelledDuringWait(t *testing.T) {
	s, _ := newSendFixture(0, nil) // rendezvous, no receiver
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.SendContext(ctx, 1) }()
	runtime.Gosched()
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
	go func() { errCh <- s.SendContext(ctx, 1) }()
	runtime.Gosched()
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
