package spmc_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/spmc"
)

func newHubTx[T any](t *testing.T, capacity int) (*spmc.Hub[T], *spmc.Sender[T]) {
	t.Helper()
	h := spmc.New[T](capacity)
	return h, h.Sender()
}

func newRx[T any](t *testing.T, h *spmc.Hub[T]) *spmc.Receiver[T] {
	t.Helper()
	return h.Receiver()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { spmc.New[int](-1) })
}

func TestSendRecvSingleReceiver(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	rx := newRx(t, h)
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
	h, tx := newHubTx[int](t, 0)
	rx := newRx(t, h)
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
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
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
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	require.NoError(t, tx.TrySend(1))
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	tx.Close()
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)
}

func TestTryRecv(t *testing.T) {
	h, tx := newHubTx[int](t, 2)
	rx := newRx(t, h)
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
	h, tx := newHubTx[int](t, 1)
	_ = newRx(t, h)
	require.NoError(t, tx.Send(1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextCancel(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx := newRx(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, v)
}

func TestSenderCloseDrainsBuffer(t *testing.T) {
	h, tx := newHubTx[int](t, 3)
	rx := newRx(t, h)
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
	h, tx := newHubTx[int](t, 1)
	_ = newRx(t, h)
	tx.Close()
	assert.NotPanics(t, func() { tx.Close() })
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestReceiverCloseIdempotent(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx := newRx(t, h)
	rx.Close()
	assert.NotPanics(t, func() { rx.Close() })
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestReceiverCloseDoesNotAffectOthers(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)

	rx1.Close()

	require.NoError(t, tx.Send(42))
	v, err := rx2.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	_, err = rx1.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllReceiversClosedSenderSeesClosed(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	rx1.Close()
	rx2.Close()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	err := tx.SendContext(context.Background(), 1)
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllReceiversClosedUnblocksPendingSend(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	require.NoError(t, tx.Send(1))
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	rx1.Close()
	rx2.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

func TestTrySendWithoutConsumerReportsNotReady(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrNotReady)
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrNotReady)

	rx := newRx(t, h)
	require.NoError(t, tx.TrySend(10))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 10, v)
}

func TestSendContextBlocksWithoutConsumer(t *testing.T) {
	_, tx := newHubTx[int](t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSendUnblocksOnFirstConsumer(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	done := make(chan error, 1)
	go func() { done <- tx.Send(42) }()
	rx := newRx(t, h)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)
	require.NoError(t, <-done)
}

func TestConsumerSharesQueue(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	rx3 := newRx(t, h)

	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	require.NoError(t, tx.Send(3))

	collect := func(rx *spmc.Receiver[int]) int {
		v, err := rx.Recv()
		require.NoError(t, err)
		return v
	}
	got := map[int]bool{}
	got[collect(rx1)] = true
	got[collect(rx2)] = true
	got[collect(rx3)] = true
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true}, got)
}

func TestReceiverAfterAllClosedIsPreClosed(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx := newRx(t, h)
	rx.Close()
	rx2 := h.Receiver()
	_, err := rx2.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestWorkDistribution(t *testing.T) {
	h, tx := newHubTx[int](t, 16)
	const workers = 4
	const items = 200

	var consumed int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		w := newRx(t, h)
		go func(w *spmc.Receiver[int]) {
			defer wg.Done()
			defer w.Close()
			for {
				_, err := w.Recv()
				if err != nil {
					return
				}
				atomic.AddInt64(&consumed, 1)
			}
		}(w)
	}

	for i := 0; i < items; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	wg.Wait()
	assert.Equal(t, int64(items), consumed)
}

func TestChanClosesOnSenderClose(t *testing.T) {
	h, tx := newHubTx[int](t, 2)
	rx := newRx(t, h)
	ch := rx.Chan()
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	tx.Close()
	got := []int{}
	for v := range ch {
		got = append(got, v)
	}
	assert.Equal(t, []int{1, 2}, got)
}

func TestChanIsShared(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	assert.Equal(t, rx1.Chan(), rx2.Chan())
}

func TestRecvReturnsClosedAfterReceiverClose(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx := newRx(t, h)
	rx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderIsIdempotent(t *testing.T) {
	h := spmc.New[int](1)
	assert.Same(t, h.Sender(), h.Sender())
}

func TestSenderReceiverAfterHubCloseAreClosed(t *testing.T) {
	h := spmc.New[int](1)
	h.Close()
	tx := h.Sender()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	rx := h.Receiver()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseAbandonsBuffer(t *testing.T) {
	h, tx := newHubTx[int](t, 4)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	h.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseUnblocksSender(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	_ = newRx(t, h)
	require.NoError(t, tx.Send(1))
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	h.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
	assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
}

func TestHubCloseUnblocksReceivers(t *testing.T) {
	h, _ := newHubTx[int](t, 1)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	errCh := make(chan error, 2)
	go func() {
		_, err := rx1.Recv()
		errCh <- err
	}()
	go func() {
		_, err := rx2.Recv()
		errCh <- err
	}()
	h.Close()
	for i := 0; i < 2; i++ {
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
	}
}

func TestHubCloseIdempotent(t *testing.T) {
	h := spmc.New[int](1)
	assert.NotPanics(t, func() {
		h.Close()
		h.Close()
	})
}

// TestSenderCloseWithoutReceiversReturnsClosed verifies that once
// Sender.Close has fired, subsequent Send/TrySend/SendContext calls return
// ErrClosed even when no receiver was ever registered (so neither dead nor
// rxReady is closed). Earlier versions checked chClosed only after waiting
// on rxReady, so Send would block forever, TrySend would report ErrFull,
// and SendContext would wait for context cancellation.
func TestSenderCloseWithoutReceiversReturnsClosed(t *testing.T) {
	t.Run("Send", func(t *testing.T) {
		_, tx := newHubTx[int](t, 1)
		tx.Close()
		assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	})
	t.Run("TrySend", func(t *testing.T) {
		_, tx := newHubTx[int](t, 1)
		tx.Close()
		assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	})
	t.Run("SendContext", func(t *testing.T) {
		_, tx := newHubTx[int](t, 1)
		tx.Close()
		assert.ErrorIs(t, tx.SendContext(context.Background(), 1), gochan.ErrClosed)
	})
}

// TestHubCloseRaceWithBlockedSender stresses the Hub.Close / blocked-Send
// race. Before the sendMu fix, Hub.Close could close s.ch while a blocked
// producer was still parked on the `s.ch <- v` arm of its select, causing a
// `send on closed channel` panic. Loop many iterations to exercise both
// scheduling orders.
func TestHubCloseRaceWithBlockedSender(t *testing.T) {
	for i := 0; i < 2000; i++ {
		h, tx := newHubTx[int](t, 1)
		_ = newRx(t, h)                // register a receiver so Send progresses past rxReady
		require.NoError(t, tx.Send(1)) // fill buffer; next Send must block
		errCh := make(chan error, 1)
		go func() { errCh <- tx.Send(2) }()
		h.Close()
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
		assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
	}
}

func TestCloseBlockedReceiverDoesNotSteal(t *testing.T) {
	// Closing one of two receivers while it is parked in Recv must wake it
	// with ErrClosed and must not let it consume the next value the sender
	// produces — that value must go to the still-open receiver.
	for i := 0; i < 1000; i++ {
		h, tx := newHubTx[int](t, 0) // rendezvous
		rx1 := h.Receiver()
		rx2 := h.Receiver()

		rx1Done := make(chan error, 1)
		go func() {
			_, err := rx1.Recv()
			rx1Done <- err
		}()
		type rx2Result struct {
			v   int
			err error
		}
		rx2Done := make(chan rx2Result, 1)
		go func() {
			v, err := rx2.Recv()
			rx2Done <- rx2Result{v, err}
		}()

		rx1.Close()
		require.NoError(t, tx.Send(42))

		select {
		case err := <-rx1Done:
			require.ErrorIsf(t, err, gochan.ErrClosed, "iter %d: closed rx1 stole the value", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx1.Recv never returned (likely stuck because Close did not wake it)", i)
		}
		select {
		case r := <-rx2Done:
			require.NoErrorf(t, r.err, "iter %d: live rx2 got error", i)
			require.Equalf(t, 42, r.v, "iter %d", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx2.Recv never returned", i)
		}
		h.Close()
	}
}

func TestCloseBlockedReceiverContextDoesNotSteal(t *testing.T) {
	// Same as above for RecvContext.
	for i := 0; i < 1000; i++ {
		h, tx := newHubTx[int](t, 0)
		rx1 := h.Receiver()
		rx2 := h.Receiver()

		ctx := context.Background()
		rx1Done := make(chan error, 1)
		go func() {
			_, err := rx1.RecvContext(ctx)
			rx1Done <- err
		}()
		type rx2Result struct {
			v   int
			err error
		}
		rx2Done := make(chan rx2Result, 1)
		go func() {
			v, err := rx2.RecvContext(ctx)
			rx2Done <- rx2Result{v, err}
		}()

		rx1.Close()
		require.NoError(t, tx.Send(7))

		select {
		case err := <-rx1Done:
			require.ErrorIsf(t, err, gochan.ErrClosed, "iter %d: closed rx1 stole the value", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx1.RecvContext never returned", i)
		}
		select {
		case r := <-rx2Done:
			require.NoErrorf(t, r.err, "iter %d", i)
			require.Equalf(t, 7, r.v, "iter %d", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx2.RecvContext never returned", i)
		}
		h.Close()
	}
}

func TestHubCloseAllowsChanDrain(t *testing.T) {
	// Recv-style callers see ErrClosed immediately after Hub.Close, but
	// Chan() consumers can still drain anything buffered before the close —
	// the underlying channel is closed (drain-then-exit), not abandoned.
	h, tx := newHubTx[int](t, 4)
	rx := newRx(t, h)
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
