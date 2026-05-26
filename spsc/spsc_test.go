package spsc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/spsc"
)

func TestImplementsCommonInterfaces(t *testing.T) {
	tx, rx := spsc.NewBounded[int](1)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { spsc.NewBounded[int](-1) })
}

func TestSendRecvFIFO(t *testing.T) {
	tx, rx := spsc.NewBounded[int](4)
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
	tx, rx := spsc.NewBounded[int](0)
	// With cap=0 there is no buffer slot, so TrySend with no parked receiver
	// must return ErrFull.
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrFull)
	// A blocking Send must wait for Recv; after the handoff both complete.
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
	tx, rx := spsc.NewBounded[int](1)
	require.NoError(t, tx.Send(1))
	// Buffer is full: TrySend reports ErrFull, proving Send would block.
	assert.ErrorIs(t, tx.TrySend(99), gochan.ErrFull)
	done := make(chan error, 1)
	go func() { done <- tx.Send(2) }()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	require.NoError(t, <-done)
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestTrySend(t *testing.T) {
	tx, rx := spsc.NewBounded[int](1)
	require.NoError(t, tx.TrySend(1))
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	tx.Close()
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)
}

func TestTryRecv(t *testing.T) {
	tx, rx := spsc.NewBounded[int](2)
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
	tx, _ := spsc.NewBounded[int](1)
	require.NoError(t, tx.Send(1)) // fills buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSendContextAlreadyCancelled(t *testing.T) {
	tx, _ := spsc.NewBounded[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tx.SendContext(ctx, 1)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRecvContextCancel(t *testing.T) {
	_, rx := spsc.NewBounded[int](1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	tx, rx := spsc.NewBounded[int](1)
	require.NoError(t, tx.Send(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, v)
}

func TestSenderCloseDrainsBuffer(t *testing.T) {
	tx, rx := spsc.NewBounded[int](3)
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
	tx, _ := spsc.NewBounded[int](1)
	tx.Close()
	assert.NotPanics(t, func() { tx.Close() })
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestReceiverCloseUnblocksSender(t *testing.T) {
	tx, rx := spsc.NewBounded[int](1)
	require.NoError(t, tx.Send(1)) // fills the buffer; next Send must block
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	rx.Close()
	// Whether the goroutine had reached its blocking select or not, Send must
	// observe rxDone and return ErrClosed.
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
	assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
}

func TestReceiverCloseIdempotent(t *testing.T) {
	_, rx := spsc.NewBounded[int](1)
	rx.Close()
	assert.NotPanics(t, func() { rx.Close() })
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestChanClosesOnSenderClose(t *testing.T) {
	tx, rx := spsc.NewBounded[int](2)
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

func TestRecvReturnsClosedAfterReceiverClose(t *testing.T) {
	_, rx := spsc.NewBounded[int](1)
	rx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestChanReturnsSameInstance(t *testing.T) {
	_, rx := spsc.NewBounded[int](1)
	assert.Equal(t, rx.Chan(), rx.Chan())
}

func TestStreamingPipeline(t *testing.T) {
	tx, rx := spsc.NewBounded[int](8)
	const n = 100
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer tx.Close()
		for i := 0; i < n; i++ {
			require.NoError(t, tx.Send(i))
		}
	}()
	got := []int{}
	for {
		v, err := rx.Recv()
		if err != nil {
			break
		}
		got = append(got, v)
	}
	wg.Wait()
	require.Len(t, got, n)
	for i, v := range got {
		assert.Equal(t, i, v)
	}
}
