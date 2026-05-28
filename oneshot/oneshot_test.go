package oneshot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
)

func newPair[T any](t *testing.T) (tx *Sender[T], rx *Receiver[T]) {
	t.Helper()
	return New[T]()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	tx, rx := newPair[int](t)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestSendThenRecv(t *testing.T) {
	tx, rx := newPair[int](t)
	require.NoError(t, tx.Send(42))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)
}

func TestSendDoesNotBlockOnReceiver(t *testing.T) {
	tx, rx := newPair[string](t)
	done := make(chan struct{})
	go func() {
		_ = tx.Send("hi")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Send blocked waiting for Recv")
	}
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, "hi", v)
}

func TestRecvBlocksUntilSend(t *testing.T) {
	tx, rx := newPair[int](t)
	type result struct {
		v   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := rx.Recv()
		ch <- result{v, err}
	}()
	select {
	case <-ch:
		t.Fatal("Recv returned before Send")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Send(7))
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		assert.Equal(t, 7, r.v)
	case <-time.After(time.Second):
		t.Fatal("Recv did not return after Send")
	}
}

func TestDoubleSendErrClosed(t *testing.T) {
	tx, _ := newPair[int](t)
	require.NoError(t, tx.Send(1))
	assert.ErrorIs(t, tx.Send(2), gochan.ErrClosed)
}

func TestSenderCloseBeforeSendCausesRecvErrClosed(t *testing.T) {
	tx, rx := newPair[int](t)
	tx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestSenderCloseAfterSendIsNoop(t *testing.T) {
	tx, rx := newPair[int](t)
	require.NoError(t, tx.Send(5))
	tx.Close()
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 5, v)
}

func TestSenderCloseIdempotent(t *testing.T) {
	tx, _ := newPair[int](t)
	assert.NotPanics(t, func() {
		tx.Close()
		tx.Close()
	})
}

func TestReceiverCloseBeforeSend(t *testing.T) {
	tx, rx := newPair[int](t)
	rx.Close()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
}

func TestReceiverCloseUnblocksRecv(t *testing.T) {
	_, rx := newPair[int](t)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := rx.Recv()
		done <- err
	}()
	<-started
	rx.Close()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, gochan.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Recv did not return after Close")
	}
}

func TestReceiverCloseIdempotent(t *testing.T) {
	_, rx := newPair[int](t)
	assert.NotPanics(t, func() {
		rx.Close()
		rx.Close()
	})
}

func TestTryRecvEmpty(t *testing.T) {
	_, rx := newPair[int](t)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
}

func TestTryRecvValue(t *testing.T) {
	tx, rx := newPair[int](t)
	require.NoError(t, tx.Send(11))
	v, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 11, v)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestTryRecvAfterSenderClose(t *testing.T) {
	tx, rx := newPair[int](t)
	tx.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSendContextCancelledReturnsCtxErr(t *testing.T) {
	tx, rx := newPair[int](t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, tx.SendContext(ctx, 1), context.Canceled)
	// Value was not deposited; sender still usable.
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
	require.NoError(t, tx.Send(1))
}

func TestSendContextOK(t *testing.T) {
	tx, rx := newPair[int](t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, tx.SendContext(ctx, 7))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 7, v)
}

func TestRecvContextCancel(t *testing.T) {
	tx, rx := newPair[int](t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	require.NoError(t, tx.Send(3))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 3, v)
}

func TestRecvContextSucceeds(t *testing.T) {
	tx, rx := newPair[int](t)
	recving := make(chan struct{})
	go func() {
		<-recving
		_ = tx.Send(9)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	close(recving)
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 9, v)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	// When both ctx is cancelled and a value is available, RecvContext must
	// deliver the value rather than report ctx.Err().
	for i := 0; i < 200; i++ {
		tx, rx := newPair[int](t)
		require.NoError(t, tx.Send(42))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		v, err := rx.RecvContext(ctx)
		require.NoError(t, err)
		assert.Equal(t, 42, v)
	}
}

func TestReceiverCloseAfterSendDropsValue(t *testing.T) {
	// rx.Close after a successful Send must drop the pending value (per spec).
	type big struct{ _ [1024]byte }
	tx, rx := newPair[*big](t)
	require.NoError(t, tx.Send(&big{}))
	rx.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestChanDelivery(t *testing.T) {
	tx, rx := newPair[int](t)
	c := rx.Chan()
	go func() { _ = tx.Send(123) }()
	v, ok := <-c
	require.True(t, ok)
	assert.Equal(t, 123, v)
	_, ok = <-c
	assert.False(t, ok, "channel should be closed after delivery")
}

func TestChanIsStable(t *testing.T) {
	_, rx := newPair[int](t)
	c := rx.Chan()
	assert.True(t, c == rx.Chan(), "Chan() returned different channels on repeated calls")
}

func TestTrySend(t *testing.T) {
	tx, rx := newPair[int](t)
	require.NoError(t, tx.TrySend(7))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	assert.ErrorIs(t, tx.TrySend(8), gochan.ErrClosed)
}

// TestChanLateRegistrationAfterSend covers Chan's late-registration branch
// where a value is already in the slot: the new userCh receives the value
// inline and is then closed.
func TestChanLateRegistrationAfterSend(t *testing.T) {
	tx, rx := newPair[int](t)
	require.NoError(t, tx.Send(42))
	c := rx.Chan()
	v, ok := <-c
	require.True(t, ok)
	assert.Equal(t, 42, v)
	_, ok = <-c
	assert.False(t, ok)
}

// TestChanLateRegistrationAfterSenderClose covers Chan's late-registration
// branch where the pair is terminal but no value is available: the new
// userCh closes empty.
func TestChanLateRegistrationAfterSenderClose(t *testing.T) {
	tx, rx := newPair[int](t)
	tx.Close()
	c := rx.Chan()
	_, ok := <-c
	assert.False(t, ok)
}

// TestReceiverCloseClosesUserCh covers the Receiver.Close path where userCh
// exists (registered via Chan) and the pair was not yet terminal — Close
// must close userCh.
func TestReceiverCloseClosesUserCh(t *testing.T) {
	_, rx := newPair[int](t)
	c := rx.Chan()
	rx.Close()
	_, ok := <-c
	assert.False(t, ok)
}

func TestChanClosedOnSenderClose(t *testing.T) {
	tx, rx := newPair[int](t)
	c := rx.Chan()
	tx.Close()
	select {
	case _, ok := <-c:
		assert.False(t, ok, "expected closed channel with no value")
	case <-time.After(time.Second):
		t.Fatal("channel not closed after sender Close")
	}
}

func TestSendRecvCloseRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		tx, rx := newPair[int](t)
		var wg sync.WaitGroup
		wg.Add(2)
		var sendErr error
		go func() {
			defer wg.Done()
			sendErr = tx.Send(1)
		}()
		go func() {
			defer wg.Done()
			rx.Close()
		}()
		wg.Wait()
		if sendErr != nil {
			assert.ErrorIs(t, sendErr, gochan.ErrClosed)
		}
	}
}
