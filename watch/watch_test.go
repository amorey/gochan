package watch_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/watch"
)

func newHub[T any](t *testing.T, initial T) *watch.Hub[T] {
	t.Helper()
	return watch.New[T](initial)
}

func newTx[T any](t *testing.T, h *watch.Hub[T]) *watch.Sender[T] {
	t.Helper()
	return h.Sender()
}

func newRx[T any](t *testing.T, h *watch.Hub[T]) *watch.Receiver[T] {
	t.Helper()
	return h.Receiver()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h := newHub[int](t, 0)
	tx := newTx(t, h)
	rx := newRx(t, h)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestSenderSingleton(t *testing.T) {
	h := newHub[int](t, 0)
	assert.Same(t, h.Sender(), h.Sender())
}

func TestFreshReceiverSeesInitialValue(t *testing.T) {
	h := newHub[string](t, "seed")
	rx := newRx(t, h)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, "seed", v)
}

func TestLateSubscriberSeesCurrentValue(t *testing.T) {
	h := newHub[int](t, 0)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(7))
	require.NoError(t, tx.Send(42))

	rx := newRx(t, h)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v, "late subscriber should see the most recent value, not initial or intermediates")
}

func TestRecvBlocksUntilChange(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	tx := newTx(t, h)

	// Drain the initial value.
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)

	got := make(chan int, 1)
	go func() {
		v, err := rx.Recv()
		require.NoError(t, err)
		got <- v
	}()

	require.NoError(t, tx.Send(2))
	select {
	case v := <-got:
		assert.Equal(t, 2, v)
	case <-time.After(time.Second):
		t.Fatal("Recv did not unblock after Send")
	}
}

func TestSendCoalescesIntermediateValues(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)

	// Drain initial.
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 0, v)

	// Publish a burst before the next Recv.
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	require.NoError(t, tx.Send(3))

	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 3, v, "should see only the most recent value")

	// And then nothing further is queued.
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
}

func TestSendNeverBlocks(t *testing.T) {
	h := newHub[int](t, 0)
	tx := newTx(t, h)
	// No receivers; Send must still succeed and not block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			require.NoError(t, tx.Send(i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Send blocked with no receivers")
	}
}

func TestTrySendEquivalentToSend(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)

	// Drain initial.
	_, _ = rx.Recv()

	require.NoError(t, tx.TrySend(11))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 11, v)

	tx.Close()
	assert.ErrorIs(t, tx.TrySend(0), gochan.ErrClosed)
}

func TestTryRecvEmptyThenValueThenClosed(t *testing.T) {
	h := newHub[int](t, 99)
	rx := newRx(t, h)
	tx := newTx(t, h)

	// Initial value is observable via TryRecv.
	v, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 99, v)

	// No new value yet.
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)

	require.NoError(t, tx.Send(7))
	v, err = rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 7, v)

	tx.Close()
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderCloseDeliversFinalValueOnce(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	tx := newTx(t, h)
	// Drain initial.
	_, _ = rx.Recv()

	require.NoError(t, tx.Send(2))
	tx.Close()
	assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)

	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v, "final value should be delivered once")

	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderCloseCaughtUpReceiverSeesErrClosed(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	tx := newTx(t, h)

	// Drain initial — receiver is now caught up.
	_, _ = rx.Recv()

	tx.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseLiveReceiverDrainsFinalValue(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	tx := newTx(t, h)
	// Drain initial.
	_, _ = rx.Recv()
	require.NoError(t, tx.Send(42))

	h.Close()
	assert.ErrorIs(t, tx.Send(0), gochan.ErrClosed)

	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestPostHubCloseReceiverDeliversFinalThenClosed(t *testing.T) {
	h := newHub[int](t, 5)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(6))
	h.Close()

	rx := newRx(t, h) // obtained after hub close
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 6, v, "post-close receiver should see the final value")

	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 0)
	h.Close()
	h.Close()
}

func TestSenderCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 0)
	tx := newTx(t, h)
	tx.Close()
	tx.Close()
}

func TestReceiverCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	rx.Close()
	rx.Close()
}

func TestReceiverCloseAbandonsPendingValue(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	// Don't drain initial.
	rx.Close()

	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestReceiverCloseDoesNotAffectOthers(t *testing.T) {
	h := newHub[int](t, 0)
	a := newRx(t, h)
	b := newRx(t, h)
	tx := newTx(t, h)

	// Drain initial on both.
	_, _ = a.Recv()
	_, _ = b.Recv()

	a.Close()
	require.NoError(t, tx.Send(2))

	_, err := a.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)

	v, err := b.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestReceiverCloseDoesNotCloseSender(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)
	rx.Close()
	// Sender keeps publishing for future subscribers.
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))

	rx2 := newRx(t, h)
	v, err := rx2.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestReceiverCloseWakesBlockedRecv(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	// Drain initial so the next Recv parks.
	_, _ = rx.Recv()

	done := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		done <- err
	}()
	rx.Close()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, gochan.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Recv did not return after Receiver.Close")
	}
}

func TestSenderCloseWakesBlockedRecv(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)
	_, _ = rx.Recv()

	done := make(chan error, 1)
	go func() {
		_, err := rx.Recv()
		done <- err
	}()
	tx.Close()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, gochan.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Recv did not return after Sender.Close")
	}
}

func TestRecvContextCancel(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	_, _ = rx.Recv()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(ctx)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("RecvContext did not return after cancel")
	}
}

func TestRecvContextPrefersValue(t *testing.T) {
	h := newHub[int](t, 7)
	rx := newRx(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, v)
}

func TestSendContextRespectsCanceled(t *testing.T) {
	h := newHub[int](t, 0)
	_ = newRx(t, h)
	tx := newTx(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, tx.SendContext(ctx, 1), context.Canceled)
}

func TestSendContextDeliversWhenLive(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)
	_, _ = rx.Recv()
	require.NoError(t, tx.SendContext(context.Background(), 99))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 99, v)
}

func TestChanDelivers(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)
	ch := rx.Chan()

	// Initial value is the first delivery.
	select {
	case v, ok := <-ch:
		require.True(t, ok)
		assert.Equal(t, 0, v)
	case <-time.After(time.Second):
		t.Fatal("initial value not delivered on Chan")
	}

	require.NoError(t, tx.Send(1))
	select {
	case v, ok := <-ch:
		require.True(t, ok)
		assert.Equal(t, 1, v)
	case <-time.After(time.Second):
		t.Fatal("Send not delivered on Chan")
	}
}

func TestChanClosesOnSenderClose(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)
	ch := rx.Chan()
	// Drain initial.
	<-ch

	tx.Close()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Chan did not close after Sender.Close")
	}
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	ch := rx.Chan()
	<-ch
	rx.Close()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Chan did not close after Receiver.Close")
	}
}

func TestChanReturnsSame(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	a := rx.Chan()
	b := rx.Chan()
	assert.Equal(t, a, b)
}

// TestChanCoalescesWhileConsumerSlow asserts that when the Chan
// consumer is not ready to receive, a subsequent Send replaces the
// in-flight value rather than being observed as an intermediate
// stale value. The feeder must not commit a value as "seen" before
// it is actually delivered to the consumer.
func TestChanCoalescesWhileConsumerSlow(t *testing.T) {
	h := newHub[int](t, 0)
	rx := newRx(t, h)
	tx := newTx(t, h)

	parked := make(chan struct{}, 8)
	rx.SetFeederParkedHook(func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	})
	ch := rx.Chan()

	// Drain initial; feeder will park on send then loop and park on
	// notify. Wait for the first park signal so we know the feeder
	// has run at least once.
	<-parked
	require.Equal(t, 0, <-ch)

	// Publish v=1. Wait until the feeder has snapshotted it and is
	// parked on the send select — without this synchronization the
	// feeder might not have committed to v=1 yet and the test would
	// not exercise the bug.
	require.NoError(t, tx.Send(1))
	<-parked

	// Publish v=2 while the feeder is still parked trying to
	// deliver 1. The feeder should observe the new notify and
	// restart, replacing the in-flight value with 2.
	require.NoError(t, tx.Send(2))
	<-parked

	// The consumer's next read must see 2, not the stale 1.
	v, ok := <-ch
	require.True(t, ok)
	assert.Equal(t, 2, v)
}

func TestConcurrentPublishersOnSingletonSender(t *testing.T) {
	// The sender is a singleton handle safe to share across goroutines.
	const writers = 8
	const writesPer = 200
	h := newHub[int](t, -1)
	tx := newTx(t, h)
	rx := newRx(t, h)

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < writesPer; j++ {
				require.NoError(t, tx.Send(i*writesPer+j))
			}
		}()
	}
	wg.Wait()
	tx.Close()

	// Drain everything we observe; just verify no deadlock and final
	// state is consistent (last Recv before ErrClosed is the latest
	// value the receiver caught up to).
	var observed int
	for {
		_, err := rx.Recv()
		if errors.Is(err, gochan.ErrClosed) {
			break
		}
		require.NoError(t, err)
		observed++
	}
	assert.LessOrEqual(t, observed, writers*writesPer+1, "should not see more values than sent (+1 initial)")
	assert.GreaterOrEqual(t, observed, 1, "should observe at least one value")
}

func TestConcurrentReceivers(t *testing.T) {
	const readers = 6
	const sends = 500
	h := newHub[int](t, 0)
	tx := newTx(t, h)

	rxs := make([]*watch.Receiver[int], readers)
	for i := range rxs {
		rxs[i] = newRx(t, h)
	}

	var seen [readers]atomic.Int64
	var wg sync.WaitGroup
	wg.Add(readers)
	for i, rx := range rxs {
		i, rx := i, rx
		go func() {
			defer wg.Done()
			for {
				_, err := rx.Recv()
				if errors.Is(err, gochan.ErrClosed) {
					return
				}
				require.NoError(t, err)
				seen[i].Add(1)
			}
		}()
	}

	for i := 1; i <= sends; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	wg.Wait()

	for i := 0; i < readers; i++ {
		got := seen[i].Load()
		assert.GreaterOrEqual(t, got, int64(1), "receiver %d saw nothing", i)
		assert.LessOrEqual(t, got, int64(sends+1), "receiver %d saw more values than sent", i)
	}
}

func TestEachReceiverIndependentLastSeen(t *testing.T) {
	h := newHub[int](t, 0)
	a := newRx(t, h)
	tx := newTx(t, h)
	// Drain initial on a.
	_, _ = a.Recv()
	require.NoError(t, tx.Send(1))
	// a sees 1.
	v, _ := a.Recv()
	assert.Equal(t, 1, v)

	// b registers now and should see current=1 immediately, independent of a.
	b := newRx(t, h)
	v, err := b.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
}
