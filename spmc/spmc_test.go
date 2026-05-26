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

func TestImplementsCommonInterfaces(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { spmc.NewBounded[int](-1) })
}

func TestSendRecvSingleReceiver(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](0)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	require.NoError(t, tx.TrySend(1))
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	tx.Close()
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)
}

func TestTryRecv(t *testing.T) {
	tx := spmc.NewBounded[int](2)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](1)
	_ = tx.Consumer()
	require.NoError(t, tx.Send(1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextCancel(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	require.NoError(t, tx.Send(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, v)
}

func TestSenderCloseDrainsBuffer(t *testing.T) {
	tx := spmc.NewBounded[int](3)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](1)
	_ = tx.Consumer()
	tx.Close()
	assert.NotPanics(t, func() { tx.Close() })
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestReceiverCloseIdempotent(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	rx.Close()
	assert.NotPanics(t, func() { rx.Close() })
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestReceiverCloseDoesNotAffectOthers(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	rx1 := tx.Consumer()
	rx2 := tx.Consumer()

	rx1.Close()

	require.NoError(t, tx.Send(42))
	v, err := rx2.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	_, err = rx1.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllReceiversClosedSenderSeesClosed(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx1 := tx.Consumer()
	rx2 := tx.Consumer()
	rx1.Close()
	rx2.Close()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	err := tx.SendContext(context.Background(), 1)
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllReceiversClosedUnblocksPendingSend(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx1 := tx.Consumer()
	rx2 := tx.Consumer()
	require.NoError(t, tx.Send(1))
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	rx1.Close()
	rx2.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

func TestTrySendWithoutConsumerReportsFull(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	// No consumer registered yet: TrySend must report ErrFull regardless of
	// the underlying buffer capacity, so callers don't silently buffer work
	// for nobody.
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrFull)
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)

	rx := tx.Consumer()
	require.NoError(t, tx.TrySend(10))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 10, v)
}

func TestSendContextBlocksWithoutConsumer(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSendUnblocksOnFirstConsumer(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	done := make(chan error, 1)
	go func() { done <- tx.Send(42) }()
	// Until a consumer registers, Send must block. Once one does, Send
	// proceeds (the buffer has room) and the value is observable via Recv.
	rx := tx.Consumer()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)
	require.NoError(t, <-done)
}

func TestConsumerSharesQueue(t *testing.T) {
	tx := spmc.NewBounded[int](4)
	rx1 := tx.Consumer()
	rx2 := tx.Consumer()
	rx3 := tx.Consumer()

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

func TestConsumerAfterAllClosedReturnsClosed(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	rx.Close()
	again := tx.Consumer()
	_, err := again.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestWorkDistribution(t *testing.T) {
	tx := spmc.NewBounded[int](16)
	const workers = 4
	const items = 200

	var consumed int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		w := tx.Consumer()
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
	tx := spmc.NewBounded[int](2)
	rx := tx.Consumer()
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
	tx := spmc.NewBounded[int](1)
	rx1 := tx.Consumer()
	rx2 := tx.Consumer()
	assert.Equal(t, rx1.Chan(), rx2.Chan())
}

func TestRecvReturnsClosedAfterReceiverClose(t *testing.T) {
	tx := spmc.NewBounded[int](1)
	rx := tx.Consumer()
	rx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}
