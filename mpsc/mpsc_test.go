package mpsc_test

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/mpsc"
)

func newHubRx[T any](t *testing.T, capacity int) (*mpsc.Hub[T], *mpsc.Receiver[T]) {
	t.Helper()
	h := mpsc.New[T](capacity)
	return h, h.Receiver()
}

func newTx[T any](t *testing.T, h *mpsc.Hub[T]) *mpsc.Sender[T] {
	t.Helper()
	return h.Sender()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { mpsc.New[int](-1) })
}

func TestSendRecvSingleSender(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx := newTx(t, h)
	for i := 0; i < 4; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	for i := 0; i < 4; i++ {
		v, err := rx.Recv()
		require.NoError(t, err)
		assert.Equal(t, i, v)
	}
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestRendezvous(t *testing.T) {
	h, rx := newHubRx[int](t, 0)
	tx := newTx(t, h)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrFull)
	sent := make(chan struct{})
	go func() {
		require.NoError(t, tx.Send(7))
		close(sent)
	}()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	<-sent
}

func TestSendBlocksWhenFull(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	assert.ErrorIs(t, tx.TrySend(99), gochan.ErrFull)
	done := make(chan error, 1)
	go func() { done <- tx.Send(2) }()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	require.NoError(t, <-done)
}

func TestTrySend(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.TrySend(1))
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	tx.Close()
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)
}

func TestTryRecv(t *testing.T) {
	h, rx := newHubRx[int](t, 2)
	tx := newTx(t, h)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
	require.NoError(t, tx.Send(9))
	v, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 9, v)
	tx.Close()
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSendContextCancel(t *testing.T) {
	h, _ := newHubRx[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextCancel(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	_ = newTx(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, v)
}

func TestSenderCloseDrainsBuffer(t *testing.T) {
	h, rx := newHubRx[int](t, 3)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	tx.Close()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderCloseIdempotent(t *testing.T) {
	h, _ := newHubRx[int](t, 1)
	tx := newTx(t, h)
	tx.Close()
	assert.NotPanics(t, func() { tx.Close() })
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestReceiverCloseIdempotent(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	_ = newTx(t, h)
	rx.Close()
	assert.NotPanics(t, func() { rx.Close() })
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderCloseDoesNotAffectOthers(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)

	tx1.Close()

	require.NoError(t, tx2.Send(42))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	assert.ErrorIs(t, tx1.Send(99), gochan.ErrClosed)
}

func TestAllSendersClosedReceiverSeesClosed(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	tx1.Close()
	tx2.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllSendersClosedReceiverDrainsFirst(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	require.NoError(t, tx1.Send(1))
	require.NoError(t, tx2.Send(2))
	tx1.Close()
	tx2.Close()
	v, err := rx.Recv()
	require.NoError(t, err)
	v2, err := rx.Recv()
	require.NoError(t, err)
	got := []int{v, v2}
	sort.Ints(got)
	assert.Equal(t, []int{1, 2}, got)
	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestReceiverCloseUnblocksPendingSend(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	rx.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

func TestReceiverCloseSendersSeeClosed(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	tx := newTx(t, h)
	rx.Close()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.SendContext(context.Background(), 1), gochan.ErrClosed)
}

func TestTryRecvFreshHubReportsNotReady(t *testing.T) {
	_, rx := newHubRx[int](t, 4)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrNotReady)
}

func TestRecvContextBlocksOnFreshHub(t *testing.T) {
	_, rx := newHubRx[int](t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvUnblocksOnFirstSend(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	done := make(chan int, 1)
	go func() {
		v, err := rx.Recv()
		require.NoError(t, err)
		done <- v
	}()
	tx := newTx(t, h)
	require.NoError(t, tx.Send(42))
	assert.Equal(t, 42, <-done)
}

func TestFanIn(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	tx3 := newTx(t, h)

	require.NoError(t, tx1.Send(1))
	require.NoError(t, tx2.Send(2))
	require.NoError(t, tx3.Send(3))

	collect := func() int {
		v, err := rx.Recv()
		require.NoError(t, err)
		return v
	}
	got := map[int]bool{}
	got[collect()] = true
	got[collect()] = true
	got[collect()] = true
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true}, got)
}

func TestSenderAfterAllClosedIsPreClosed(t *testing.T) {
	h, _ := newHubRx[int](t, 1)
	tx := newTx(t, h)
	tx.Close()
	tx2 := h.Sender()
	assert.ErrorIs(t, tx2.Send(1), gochan.ErrClosed)
}

func TestReceiverIsIdempotent(t *testing.T) {
	h := mpsc.New[int](1)
	assert.Same(t, h.Receiver(), h.Receiver())
}

func TestSenderReceiverAfterHubCloseAreClosed(t *testing.T) {
	h := mpsc.New[int](1)
	h.Close()
	tx := h.Sender()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	rx := h.Receiver()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseAbandonsBuffer(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	h.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseUnblocksSenders(t *testing.T) {
	h, _ := newHubRx[int](t, 1)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	require.NoError(t, tx1.Send(1))
	errCh := make(chan error, 2)
	go func() { errCh <- tx1.Send(2) }()
	go func() { errCh <- tx2.Send(3) }()
	h.Close()
	for i := 0; i < 2; i++ {
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
	}
}

func TestHubCloseUnblocksReceiver(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	_ = newTx(t, h)
	errCh := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		errCh <- err
	}()
	h.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

func TestHubCloseIdempotent(t *testing.T) {
	h := mpsc.New[int](1)
	assert.NotPanics(t, func() {
		h.Close()
		h.Close()
	})
}

func TestChanClosesAfterAllSendersClose(t *testing.T) {
	h, rx := newHubRx[int](t, 4)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	ch := rx.Chan()
	require.NoError(t, tx1.Send(1))
	require.NoError(t, tx2.Send(2))
	tx1.Close()
	tx2.Close()
	got := []int{}
	for v := range ch {
		got = append(got, v)
	}
	sort.Ints(got)
	assert.Equal(t, []int{1, 2}, got)
}

func TestChanRepeatedCallsAreSame(t *testing.T) {
	h, rx := newHubRx[int](t, 1)
	assert.Equal(t, rx.Chan(), rx.Chan())
	_ = h
}

func TestHubCloseAllowsChanDrain(t *testing.T) {
	// Recv-style callers see ErrClosed immediately after Hub.Close, but
	// Chan() consumers can still drain anything buffered before the close —
	// the underlying channel is closed (drain-then-exit), not abandoned.
	h, rx := newHubRx[int](t, 4)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	ch := rx.Chan()
	h.Close()
	var got []int
	for v := range ch {
		got = append(got, v)
	}
	assert.Equal(t, []int{1, 2}, got)
}

// TestHubCloseRaceWithBlockedSender stresses Hub.Close vs. a blocked
// producer parked on `s.ch <- v`. chMu must keep close(s.ch) from racing
// the send arm. Loop many iterations to exercise both scheduling orders.
func TestHubCloseRaceWithBlockedSender(t *testing.T) {
	for i := 0; i < 2000; i++ {
		h, _ := newHubRx[int](t, 1)
		tx := newTx(t, h)
		require.NoError(t, tx.Send(1)) // fill buffer; next Send must block
		errCh := make(chan error, 1)
		go func() { errCh <- tx.Send(2) }()
		h.Close()
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
		assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
	}
}

func TestMultiProducerFanIn(t *testing.T) {
	h, rx := newHubRx[int](t, 16)
	const producers = 8
	const itemsPer = 100

	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		tx := newTx(t, h)
		go func(p int) {
			defer wg.Done()
			defer tx.Close()
			for j := 0; j < itemsPer; j++ {
				require.NoError(t, tx.Send(p*itemsPer+j))
			}
		}(i)
	}

	var consumed int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := rx.Recv()
			if err != nil {
				return
			}
			atomic.AddInt64(&consumed, 1)
		}
	}()

	wg.Wait()
	<-done
	assert.Equal(t, int64(producers*itemsPer), consumed)
}
