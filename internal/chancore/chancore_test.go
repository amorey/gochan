package chancore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
)

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
		Ch:       make(chan int, capacity),
		Dead:     dead.Done(),
		Ready:    ready,
		ChClosed: &chClosed,
		SendLock: &mu,
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
