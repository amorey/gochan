package mpmc

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
)

func newHub[T any](t *testing.T, capacity int) *Hub[T] {
	t.Helper()
	return New[T](capacity)
}

func newTx[T any](t *testing.T, h *Hub[T]) *Sender[T] {
	t.Helper()
	return h.Sender()
}

func newRx[T any](t *testing.T, h *Hub[T]) *Receiver[T] {
	t.Helper()
	return h.Receiver()
}

func TestImplementsCommonInterfaces(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx := newRx(t, h)
	var _ gochan.Sender[int] = tx
	var _ gochan.Receiver[int] = rx
}

func TestNegativeCapacityPanics(t *testing.T) {
	assert.Panics(t, func() { New[int](-1) })
}

func TestSendRecvFIFO(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
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
	h := newHub[int](t, 0)
	tx := newTx(t, h)
	rx := newRx(t, h)

	// Cap=0, no parked receiver yet, so TrySend reports ErrFull — not
	// ErrNotReady, since the rxReady latch closed when newRx ran.
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrFull)

	done := make(chan int, 1)
	go func() {
		v, err := rx.Recv()
		assert.NoError(t, err)
		done <- v
	}()
	require.NoError(t, tx.Send(7))
	assert.Equal(t, 7, <-done)
}

func TestSendBlocksWhenFull(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(1))

	sent := make(chan error, 1)
	go func() { sent <- tx.Send(2) }()
	// Drain the first value so the blocked send can complete.
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	require.NoError(t, <-sent)
}

func TestTrySendGates(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)

	// Before any receiver registers: ErrNotReady.
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrNotReady)
	assert.ErrorIs(t, tx.TrySend(2), gochan.ErrNotReady)

	rx := newRx(t, h)

	// Latch closed; buffer is cap=1 so first send succeeds, second is ErrFull.
	require.NoError(t, tx.TrySend(10))
	assert.ErrorIs(t, tx.TrySend(11), gochan.ErrFull)

	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 10, v)

	// Receiver close → ErrClosed.
	rx.Close()
	assert.ErrorIs(t, tx.TrySend(20), gochan.ErrClosed)
}

func TestTryRecvGates(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)

	// Before any sender registers: ErrNotReady.
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrNotReady)

	tx := newTx(t, h)

	// Latch closed but buffer empty: ErrEmpty.
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrEmpty)

	require.NoError(t, tx.Send(42))
	v, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	tx.Close()
	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestTryRecvAfterReceiverClose(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newTx(t, h)
	rx := newRx(t, h)
	rx.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestTryRecvAfterHubClose(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newTx(t, h)
	rx := newRx(t, h)
	h.Close()
	_, err := rx.TryRecv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSendContextCancel(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	_ = newRx(t, h)
	require.NoError(t, tx.Send(1)) // fill buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tx.SendContext(ctx, 2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextCancel(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	_ = newTx(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecvContextPrefersValueOverCancel(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(99))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, 99, v)
}

func TestSenderCloseDoesNotAffectOthers(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)

	tx1.Close()

	require.NoError(t, tx2.Send(42))
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 42, v)

	assert.ErrorIs(t, tx1.Send(99), gochan.ErrClosed)
}

func TestReceiverCloseDoesNotAffectOthers(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
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

func TestSenderCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newRx(t, h)
	tx := newTx(t, h)
	tx.Close()
	assert.NotPanics(t, func() { tx.Close() })
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.SendContext(context.Background(), 1), gochan.ErrClosed)
}

// TestRecvContextValueArrivesDuringWait covers RecvContext's second-select
// "case v, ok := <-s.ch" arm with a real value — value is sent after the
// non-blocking probe missed, while the receiver is parked in the second
// select.
func TestRecvContextValueArrivesDuringWait(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx := newRx(t, h)
	type result struct {
		v   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := rx.RecvContext(context.Background())
		done <- result{v, err}
	}()
	runtime.Gosched()
	require.NoError(t, tx.Send(42))
	r := <-done
	require.NoError(t, r.err)
	assert.Equal(t, 42, r.v)
}

// recvOp names a blocking recv-style method on a *Receiver.
type recvOp struct {
	name string
	call func(*Receiver[int]) error
}

var recvOps = []recvOp{
	{"Recv", func(rx *Receiver[int]) error { _, err := rx.Recv(); return err }},
	{"RecvContext", func(rx *Receiver[int]) error { _, err := rx.RecvContext(context.Background()); return err }},
}

// runParkedRecvHubCloseRace spawns the recv op, ensures it has started
// (WaitGroup) and then yields (Gosched) so the goroutine reaches the
// select before Hub.Close fires. Loops to overcome scheduler bias.
func runParkedRecvHubCloseRace(t *testing.T, op recvOp) {
	t.Helper()
	for i := 0; i < 200; i++ {
		h := newHub[int](t, 0)
		_ = newTx(t, h)
		rx := newRx(t, h)
		var wg sync.WaitGroup
		wg.Add(1)
		done := make(chan error, 1)
		go func() {
			wg.Done()
			done <- op.call(rx)
		}()
		wg.Wait()
		runtime.Gosched()
		h.Close()
		assert.ErrorIs(t, <-done, gochan.ErrClosed)
	}
}

// TestRecvSelectWakesOnHubClose covers the <-s.dead.Done() select arm of
// both Recv and RecvContext — the receiver is parked when the hub fires
// dead.
func TestRecvSelectWakesOnHubClose(t *testing.T) {
	for _, op := range recvOps {
		t.Run(op.name, func(t *testing.T) { runParkedRecvHubCloseRace(t, op) })
	}
}

// TestRecvSelectWakesOnReceiverClose covers the <-rx.done.Done() select
// arm of both Recv and RecvContext — the receiver is parked when its own
// Close fires. Other tests close the receiver before calling Recv, so they
// hit the lock-free IsClosed fast path and never enter the select.
func TestRecvSelectWakesOnReceiverClose(t *testing.T) {
	for _, op := range recvOps {
		t.Run(op.name, func(t *testing.T) {
			for i := 0; i < 200; i++ {
				h := newHub[int](t, 0)
				_ = newTx(t, h)
				rx := newRx(t, h)
				var wg sync.WaitGroup
				wg.Add(1)
				done := make(chan error, 1)
				go func() {
					wg.Done()
					done <- op.call(rx)
				}()
				wg.Wait()
				runtime.Gosched()
				rx.Close()
				assert.ErrorIs(t, <-done, gochan.ErrClosed)
			}
		})
	}
}

// TestRecvContextProbeSeesChannelClosed covers RecvContext's first
// (non-blocking) select arm "case v, ok := <-s.ch" with !ok — the channel
// is already closed (all senders closed) before RecvContext runs.
func TestRecvContextProbeSeesChannelClosed(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx := newRx(t, h)
	tx.Close() // last sender close → s.ch closed
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestRecvContextSelectWakesOnSendersClosed covers RecvContext's
// second-select "case v, ok := <-s.ch" with !ok — last sender closes the
// channel while the receiver is parked, dead is not fired. Standalone
// rather than parameterized because the trigger needs the sender handle
// returned by newTx.
func TestRecvContextSelectWakesOnSendersClosed(t *testing.T) {
	for i := 0; i < 50; i++ {
		h := newHub[int](t, 0)
		tx := newTx(t, h)
		rx := newRx(t, h)
		var wg sync.WaitGroup
		wg.Add(1)
		done := make(chan error, 1)
		go func() {
			wg.Done()
			_, err := rx.RecvContext(context.Background())
			done <- err
		}()
		wg.Wait()
		runtime.Gosched()
		tx.Close() // last sender → closes s.ch, does not fire dead
		assert.ErrorIs(t, <-done, gochan.ErrClosed)
	}
}

func TestReceiverCloseIdempotent(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newTx(t, h)
	rx := newRx(t, h)
	rx.Close()
	assert.NotPanics(t, func() { rx.Close() })
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllSendersClosedReceiversDrainThenSeeClosed(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	require.NoError(t, tx1.Send(1))
	require.NoError(t, tx2.Send(2))
	tx1.Close()
	tx2.Close()

	got := []int{}
	for i := 0; i < 2; i++ {
		v, err := rx.Recv()
		require.NoError(t, err)
		got = append(got, v)
	}
	sort.Ints(got)
	assert.Equal(t, []int{1, 2}, got)

	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestAllReceiversClosedSendersSeeClosed(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	rx1.Close()
	rx2.Close()

	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.TrySend(1), gochan.ErrClosed)
	assert.ErrorIs(t, tx.SendContext(context.Background(), 1), gochan.ErrClosed)
}

func TestAllReceiversClosedUnblocksPendingSend(t *testing.T) {
	h := newHub[int](t, 1)
	tx := newTx(t, h)
	rx1 := newRx(t, h)
	rx2 := newRx(t, h)
	require.NoError(t, tx.Send(1)) // fill buffer
	errCh := make(chan error, 1)
	go func() { errCh <- tx.Send(2) }()
	rx1.Close()
	rx2.Close()
	assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
}

func TestSendUnblocksOnFirstReceiver(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
	sent := make(chan error, 1)
	go func() { sent <- tx.Send(7) }()

	rx := newRx(t, h)
	v, err := rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	require.NoError(t, <-sent)
}

func TestRecvUnblocksOnFirstSender(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
	got := make(chan int, 1)
	go func() {
		v, err := rx.Recv()
		assert.NoError(t, err)
		got <- v
	}()
	tx := newTx(t, h)
	require.NoError(t, tx.Send(42))
	assert.Equal(t, 42, <-got)
}

func TestSenderAfterAllSendersClosedIsPreClosed(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newRx(t, h)
	tx := newTx(t, h)
	tx.Close()
	tx2 := h.Sender()
	assert.ErrorIs(t, tx2.Send(1), gochan.ErrClosed)
}

func TestReceiverAfterAllReceiversClosedIsPreClosed(t *testing.T) {
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	rx.Close()
	rx2 := h.Receiver()
	_, err := rx2.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestSenderReceiverAfterHubCloseAreClosed(t *testing.T) {
	h := New[int](1)
	h.Close()
	tx := h.Sender()
	assert.ErrorIs(t, tx.Send(1), gochan.ErrClosed)
	rx := h.Receiver()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseAbandonsBufferForRecv(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	h.Close()
	_, err := rx.Recv()
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

func TestHubCloseAllowsChanDrain(t *testing.T) {
	h := newHub[int](t, 4)
	tx := newTx(t, h)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(1))
	require.NoError(t, tx.Send(2))
	ch := rx.Chan()
	h.Close()
	got := []int{}
	for v := range ch {
		got = append(got, v)
	}
	assert.Equal(t, []int{1, 2}, got)
}

func TestHubCloseUnblocksSenders(t *testing.T) {
	h := newHub[int](t, 1)
	tx1 := newTx(t, h)
	tx2 := newTx(t, h)
	_ = newRx(t, h)
	require.NoError(t, tx1.Send(1))
	errCh := make(chan error, 2)
	go func() { errCh <- tx1.Send(2) }()
	go func() { errCh <- tx2.Send(3) }()
	h.Close()
	for i := 0; i < 2; i++ {
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
	}
}

func TestHubCloseUnblocksReceivers(t *testing.T) {
	h := newHub[int](t, 1)
	_ = newTx(t, h)
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
	h := New[int](1)
	assert.NotPanics(t, func() {
		h.Close()
		h.Close()
	})
}

func TestChanClosesAfterAllSendersClose(t *testing.T) {
	h := newHub[int](t, 4)
	rx := newRx(t, h)
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
	h := newHub[int](t, 1)
	rx := newRx(t, h)
	assert.Equal(t, rx.Chan(), rx.Chan())
}

func TestWorkDistribution(t *testing.T) {
	// Every value should be delivered to exactly one receiver.
	const items = 200
	h := newHub[int](t, 16)
	tx := newTx(t, h)

	const workers = 4
	counts := make([]int64, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		rx := newRx(t, h)
		go func(i int) {
			defer wg.Done()
			for {
				if _, err := rx.Recv(); err != nil {
					return
				}
				atomic.AddInt64(&counts[i], 1)
			}
		}(i)
	}

	for i := 0; i < items; i++ {
		require.NoError(t, tx.Send(i))
	}
	tx.Close()
	wg.Wait()

	var total int64
	for _, c := range counts {
		total += c
	}
	assert.Equal(t, int64(items), total)
}

func TestMultiProducerMultiConsumer(t *testing.T) {
	// All produced values must be received exactly once across all consumers.
	const producers = 4
	const consumers = 4
	const itemsPer = 250
	const total = producers * itemsPer

	h := newHub[int](t, 32)

	// Spawn consumers first so receivers exist; otherwise producers would
	// block on the rxReady latch.
	var consumed sync.Map
	var consumedCount int64
	consumerWG := sync.WaitGroup{}
	consumerWG.Add(consumers)
	for i := 0; i < consumers; i++ {
		rx := newRx(t, h)
		go func() {
			defer consumerWG.Done()
			for {
				v, err := rx.Recv()
				if err != nil {
					return
				}
				if _, loaded := consumed.LoadOrStore(v, struct{}{}); loaded {
					t.Errorf("value %d delivered more than once", v)
					return
				}
				atomic.AddInt64(&consumedCount, 1)
			}
		}()
	}

	producerWG := sync.WaitGroup{}
	producerWG.Add(producers)
	for i := 0; i < producers; i++ {
		tx := newTx(t, h)
		go func(p int) {
			defer producerWG.Done()
			defer tx.Close()
			for j := 0; j < itemsPer; j++ {
				assert.NoError(t, tx.Send(p*itemsPer+j))
			}
		}(i)
	}

	producerWG.Wait()
	consumerWG.Wait()
	assert.Equal(t, int64(total), consumedCount)
}

// TestHubCloseRaceWithBlockedSender stresses Hub.Close vs. a blocked
// producer parked on `s.ch <- v`. chMu must keep close(s.ch) from racing
// the send arm.
func TestCloseBlockedReceiverNoValueLost(t *testing.T) {
	// Closing one of two parked receivers must wake it without deadlocking
	// the sender or losing the value: either the closing receiver picks up
	// the value successfully (race won the channel arm at the instant of
	// close) and the still-open receiver gets nothing, or it wakes with
	// ErrClosed and the value is delivered to the still-open receiver.
	for i := 0; i < 1000; i++ {
		h := newHub[int](t, 0) // rendezvous
		tx := newTx(t, h)
		rx1 := newRx(t, h)
		rx2 := newRx(t, h)

		type result struct {
			v   int
			err error
		}
		rx1Done := make(chan result, 1)
		go func() {
			v, err := rx1.Recv()
			rx1Done <- result{v, err}
		}()
		rx2Done := make(chan result, 1)
		go func() {
			v, err := rx2.Recv()
			rx2Done <- result{v, err}
		}()

		rx1.Close()
		require.NoError(t, tx.Send(42))

		var r1 result
		select {
		case r1 = <-rx1Done:
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx1.Recv never returned (likely stuck because Close did not wake it)", i)
		}
		if r1.err == nil {
			require.Equalf(t, 42, r1.v, "iter %d", i)
			h.Close()
			r2 := <-rx2Done
			require.ErrorIsf(t, r2.err, gochan.ErrClosed, "iter %d", i)
			continue
		}
		require.ErrorIsf(t, r1.err, gochan.ErrClosed, "iter %d", i)
		select {
		case r2 := <-rx2Done:
			require.NoErrorf(t, r2.err, "iter %d: live rx2 got error", i)
			require.Equalf(t, 42, r2.v, "iter %d", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx2.Recv never returned", i)
		}
		h.Close()
	}
}

func TestCloseBlockedReceiverContextNoValueLost(t *testing.T) {
	for i := 0; i < 1000; i++ {
		h := newHub[int](t, 0)
		tx := newTx(t, h)
		rx1 := newRx(t, h)
		rx2 := newRx(t, h)

		ctx := context.Background()
		type result struct {
			v   int
			err error
		}
		rx1Done := make(chan result, 1)
		go func() {
			v, err := rx1.RecvContext(ctx)
			rx1Done <- result{v, err}
		}()
		rx2Done := make(chan result, 1)
		go func() {
			v, err := rx2.RecvContext(ctx)
			rx2Done <- result{v, err}
		}()

		rx1.Close()
		require.NoError(t, tx.Send(7))

		var r1 result
		select {
		case r1 = <-rx1Done:
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx1.RecvContext never returned", i)
		}
		if r1.err == nil {
			require.Equalf(t, 7, r1.v, "iter %d", i)
			h.Close()
			r2 := <-rx2Done
			require.ErrorIsf(t, r2.err, gochan.ErrClosed, "iter %d", i)
			continue
		}
		require.ErrorIsf(t, r1.err, gochan.ErrClosed, "iter %d", i)
		select {
		case r2 := <-rx2Done:
			require.NoErrorf(t, r2.err, "iter %d", i)
			require.Equalf(t, 7, r2.v, "iter %d", i)
		case <-time.After(time.Second):
			t.Fatalf("iter %d: rx2.RecvContext never returned", i)
		}
		h.Close()
	}
}

func TestCloseBlockedReceiverPreservesBufferedOrder(t *testing.T) {
	// Closing a receiver racing a buffered value must not reorder the queue:
	// each receiver sees a strictly increasing subsequence of sends and the
	// union across receivers is exactly the original FIFO sequence with no
	// duplicates and no missing values.
	for i := 0; i < 500; i++ {
		const n = 8
		h := newHub[int](t, n)
		tx := newTx(t, h)
		rx1 := newRx(t, h)
		rx2 := newRx(t, h)
		for v := 1; v <= n; v++ {
			require.NoError(t, tx.Send(v))
		}

		rx1Done := make(chan []int, 1)
		go func() {
			var got []int
			for {
				v, err := rx1.Recv()
				if err != nil {
					rx1Done <- got
					return
				}
				got = append(got, v)
			}
		}()
		rx2Done := make(chan []int, 1)
		go func() {
			var got []int
			for {
				v, err := rx2.Recv()
				if err != nil {
					rx2Done <- got
					return
				}
				got = append(got, v)
			}
		}()

		rx1.Close()
		tx.Close()

		got1 := <-rx1Done
		got2 := <-rx2Done

		for _, seq := range [][]int{got1, got2} {
			for k := 1; k < len(seq); k++ {
				require.Greaterf(t, seq[k], seq[k-1], "iter %d: out-of-order receive %v", i, seq)
			}
		}
		seen := make(map[int]bool, n)
		for _, v := range got1 {
			require.Falsef(t, seen[v], "iter %d: duplicate value %d", i, v)
			seen[v] = true
		}
		for _, v := range got2 {
			require.Falsef(t, seen[v], "iter %d: duplicate value %d", i, v)
			seen[v] = true
		}
		require.Lenf(t, seen, n, "iter %d: missing values, got %v / %v", i, got1, got2)
	}
}

func TestHubCloseRaceWithBlockedSender(t *testing.T) {
	for i := 0; i < 2000; i++ {
		h := newHub[int](t, 1)
		tx := newTx(t, h)
		_ = newRx(t, h)
		require.NoError(t, tx.Send(1)) // fill buffer; next Send must block
		errCh := make(chan error, 1)
		go func() { errCh <- tx.Send(2) }()
		h.Close()
		assert.ErrorIs(t, <-errCh, gochan.ErrClosed)
		assert.ErrorIs(t, tx.Send(3), gochan.ErrClosed)
	}
}
