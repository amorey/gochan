package broadcast_test

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
	"github.com/amorey/gochan/broadcast"
)

func newHub[T any](t *testing.T, capacity int) *broadcast.Hub[T] {
	t.Helper()
	return broadcast.New[T](capacity)
}

func newTx[T any](t *testing.T, h *broadcast.Hub[T]) *broadcast.Sender[T] {
	t.Helper()
	return h.Sender()
}

func newRx[T any](t *testing.T, h *broadcast.Hub[T]) *broadcast.Receiver[T] {
	t.Helper()
	return h.Receiver()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
	rx := newRx(t, h)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestZeroOrNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { broadcast.New[int](0) })
	assert.Panics(t, func() { broadcast.New[int](-1) })
}

func TestSenderSingleton(t *testing.T) {
	h := newHub[int](t, 4)
	assert.Same(t, h.Sender(), h.Sender())
}

func TestFanOutDeliversToEveryReceiver(t *testing.T) {
	const items = 50
	const nrx = 4
	h := newHub[int](t, 64)
	rxs := make([]*broadcast.Receiver[int], nrx)
	for i := range rxs {
		rxs[i] = newRx(t, h)
	}
	tx := newTx(t, h)

	var wg sync.WaitGroup
	wg.Add(nrx)
	got := make([][]int, nrx)
	for i, rx := range rxs {
		i, rx := i, rx
		go func() {
			defer wg.Done()
			for {
				v, err := rx.Recv()
				if errors.Is(err, gochan.ErrClosed) {
					return
				}
				require.NoError(t, err)
				got[i] = append(got[i], v)
			}
		}()
	}

	for i := 0; i < items; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	wg.Wait()

	want := make([]int, items)
	for i := range want {
		want[i] = i
	}
	for i := range got {
		assert.Equal(t, want, got[i], "rx %d", i)
	}
}

func TestLateSubscriberSeesOnlyFutureValues(t *testing.T) {
	h := newHub[int](t, 8)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))

	rx := newRx(t, h)
	require.NoError(t, tx.Send(3))
	require.NoError(t, tx.Send(4))
	tx.Close()

	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 4, v)
	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSendNeverBlocksAndOverwrites(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)
	for i := 0; i < 5; i++ {
		require.NoError(t, tx.Send(i))
	}
	v, err := rx.Recv()
	var lagged gochan.ErrLagged
	require.True(t, errors.As(err, &lagged), "want ErrLagged, got %v", err)
	assert.Equal(t, uint64(3), lagged.Missed)
	assert.Equal(t, 0, v) // value is the zero value: lagged has no value

	// Receiver resumes at the oldest still-buffered value, which is 3
	// (values 3 and 4 remain in the size-2 ring after 5 writes).
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 4, v)
}

func TestTrySendErrFullOnEvict(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)
	require.NoError(t, tx.TrySend(1))
	require.NoError(t, tx.TrySend(2))
	// Receiver hasn't consumed anything yet; next write would evict
	// the unread value at position 0.
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrFull)

	// Drain one — TrySend can now proceed.
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	require.NoError(t, tx.TrySend(3))
}

func TestTrySendNoReceiversAlwaysSucceeds(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	require.NoError(t, tx.TrySend(1))
	require.NoError(t, tx.TrySend(2)) // overwrites; no rx so no eviction
}

func TestSendSkipsRingWhenNoReceivers(t *testing.T) {
	// Values published before any Receiver registers must not be
	// retained — late subscribers start at "now" and would never
	// observe them, so the ring would only pin payloads.
	h := newHub[*int](t, 4)
	tx := newTx(t, h)
	val := 7
	require.NoError(t, tx.Send(&val))
	require.NoError(t, tx.Send(&val))
	require.NoError(t, tx.TrySend(&val))

	rx := newRx(t, h)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
}

func TestLastReceiverLeaveClearsRing(t *testing.T) {
	// After the last receiver closes, the ring should release its
	// payloads so callers don't pin them for the hub's lifetime.
	h := newHub[*int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)
	val := 9
	require.NoError(t, tx.Send(&val))
	require.NoError(t, tx.Send(&val))
	rx.Close()

	// Re-subscribing and reading should not surface the stale values.
	rx2 := newRx(t, h)
	_, err := rx2.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)
}

func TestReceiverCloseRacingPendingValue(t *testing.T) {
	// Regression for the close-race that let a closed receiver
	// observe a pending value because rx.done wasn't re-checked
	// under mu. Each iteration: sender publishes one value, then
	// Close and Recv race.
	const iters = 2000
	for i := 0; i < iters; i++ {
		h := newHub[int](t, 4)
		tx := newTx(t, h)
		rx := newRx(t, h)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var recvErr error
		var recvVal int
		go func() {
			defer wg.Done()
			<-start
			recvVal, recvErr = rx.Recv()
		}()
		go func() {
			defer wg.Done()
			<-start
			rx.Close()
		}()

		require.NoError(t, tx.Send(7))
		close(start)
		wg.Wait()

		if recvErr == nil {
			require.Equal(t, 7, recvVal)
		} else {
			require.ErrorIs(t, recvErr, gochan.ErrClosed)
		}
		_, err := rx.Recv()
		require.ErrorIs(t, err, gochan.ErrClosed)
	}
}

func TestTrySendIgnoresClosedReceivers(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	tx := newTx(t, h)
	require.NoError(t, tx.TrySend(1))
	// Without closing rx, the second write would evict.
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrFull)
	rx.Close()
	require.NoError(t, tx.TrySend(2))
}

func TestTryRecvEmptyAndClosed(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)

	require.NoError(t, tx.Send(1))
	v, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)

	tx.Close()
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderCloseDrains(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	tx.Close()
	assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(3), gochan.ErrClosed)

	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	v, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	_, err = rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseStopsEveryHandle(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(42)) // buffered value
	h.Close()

	// Sender is closed.
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	// Receiver is closed too — does NOT drain the buffered value.
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseReturnsPreClosedReceivers(t *testing.T) {
	h := newHub[int](t, 2)
	h.Close()
	rx := newRx(t, h)
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseUnblocksChanFeeder(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	_ = newTx(t, h)
	ch := rx.Chan()
	h.Close()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel should be closed")
	case <-time.After(time.Second):
		t.Fatal("Chan feeder did not exit after Hub.Close")
	}
}

func TestHubCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 2)
	h.Close()
	h.Close()
}

func TestReceiverCloseDoesNotAffectOthers(t *testing.T) {
	h := newHub[int](t, 4)
	a := newRx(t, h)
	b := newRx(t, h)
	tx := newTx(t, h)

	require.NoError(t, tx.Send(1))
	a.Close()
	require.NoError(t, tx.Send(2))

	_, err := a.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)

	v, err := b.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	v, err = b.Recv()
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestReceiverCloseWakesBlockedRecv(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	_ = newTx(t, h)

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
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)

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
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	_ = newTx(t, h)

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
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)

	require.NoError(t, tx.Send(7))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, v)
}

func TestSendContextRespectsCanceled(t *testing.T) {
	h := newHub[int](t, 2)
	_ = newRx(t, h)
	tx := newTx(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, tx.SendContext(ctx, 1), context.Canceled)
}

func TestChanDeliversInOrder(t *testing.T) {
	h := newHub[int](t, 32)
	rx := newRx(t, h)
	tx := newTx(t, h)
	ch := rx.Chan()
	assert.Equal(t, ch, rx.Chan()) // idempotent

	go func() {
		for i := 0; i < 10; i++ {
			require.NoError(t, tx.Send(i))
		}
		tx.Close()
	}()

	var got []int
	for v := range ch {
		got = append(got, v)
	}
	want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, want, got)
}

func TestChanSilentlySkipsLagged(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	tx := newTx(t, h)
	ch := rx.Chan()

	// Write past the ring before the chan-feeder goroutine drains.
	// The feeder will see ErrLagged on its first Recv and skip;
	// downstream consumer only observes surviving values.
	for i := 0; i < 5; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()

	var got []int
	for v := range ch {
		got = append(got, v)
	}
	// After 5 writes into a cap-2 ring, 3 and 4 survive.
	// The feeder may have read some values before lag was detected,
	// but the surviving tail must end with 3, 4.
	require.NotEmpty(t, got)
	assert.Equal(t, 4, got[len(got)-1])
	for _, v := range got {
		assert.True(t, v >= 0 && v <= 4)
	}
}

func TestChanClosesOnReceiverClose(t *testing.T) {
	h := newHub[int](t, 2)
	rx := newRx(t, h)
	_ = newTx(t, h)
	ch := rx.Chan()
	rx.Close()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Chan did not close after Receiver.Close")
	}
}

func TestPostCloseReceiverIsImmediatelyClosed(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
	require.NoError(t, tx.Send(1))
	tx.Close()
	rx := newRx(t, h) // registers at writePos == 1, sender already closed
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestLagThenResumeBuffered(t *testing.T) {
	h := newHub[int](t, 3)
	rx := newRx(t, h)
	tx := newTx(t, h)
	// Write 10 values; ring of 3 → values 7,8,9 survive.
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Send(i))
	}
	v, err := rx.Recv()
	var lagged gochan.ErrLagged
	require.True(t, errors.As(err, &lagged))
	assert.Equal(t, uint64(7), lagged.Missed) // missed 0..6
	assert.Equal(t, 0, v)

	for i, want := range []int{7, 8, 9} {
		v, err := rx.Recv()
		require.NoError(t, err, "iter %d", i)
		assert.Equal(t, want, v)
	}
}

func TestConcurrentPublishers(t *testing.T) {
	const publishers = 4
	const perPub = 100
	h := newHub[int](t, 8*publishers*perPub) // big enough to avoid lag
	rx := newRx(t, h)
	tx := newTx(t, h)

	var wg sync.WaitGroup
	wg.Add(publishers)
	for p := 0; p < publishers; p++ {
		p := p
		go func() {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				require.NoError(t, tx.Send(p*perPub+i))
			}
		}()
	}
	wg.Wait()
	tx.Close()

	var count int
	seen := make(map[int]struct{})
	for {
		v, err := rx.Recv()
		if errors.Is(err, gochan.ErrClosed) {
			break
		}
		require.NoError(t, err)
		seen[v] = struct{}{}
		count++
	}
	assert.Equal(t, publishers*perPub, count)
	assert.Len(t, seen, publishers*perPub)
}

func TestConcurrentReceiversAllSeeEveryValue(t *testing.T) {
	const nrx = 8
	const items = 200
	h := newHub[int](t, 256)
	rxs := make([]*broadcast.Receiver[int], nrx)
	for i := range rxs {
		rxs[i] = newRx(t, h)
	}
	tx := newTx(t, h)

	var wg sync.WaitGroup
	wg.Add(nrx)
	counts := make([]int64, nrx)
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
				atomic.AddInt64(&counts[i], 1)
			}
		}()
	}

	for i := 0; i < items; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	wg.Wait()

	for i, c := range counts {
		assert.Equal(t, int64(items), c, "rx %d", i)
	}
}
