package spmc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/internal/parked"
)

func newHubTx[T any](t *testing.T, capacity int) (*Hub[T], *Sender[T]) {
	t.Helper()
	h := New[T](capacity)
	return h, h.Sender()
}

func newRx[T any](t *testing.T, h *Hub[T]) *Receiver[T] {
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
	assert.Panics(t, func() { New[int](-1) })
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
		assert.NoError(t, tx.Send(7))
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

// TestRecvContextCancelBeatsBufferedValue mirrors mpmc's test of the same
// name: a cancelled ctx outranks a buffered value, and the value stays in
// the queue.
func TestRecvContextCancelBeatsBufferedValue(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	require.NoError(t, tx.Send(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rx.RecvContext(ctx)
	require.ErrorIs(t, err, context.Canceled)

	v, err := rx.TryRecv()
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

	collect := func(rx *Receiver[int]) int {
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
		go func(w *Receiver[int]) {
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
	h := New[int](1)
	assert.Same(t, h.Sender(), h.Sender())
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
	h := New[int](1)
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

func TestCloseBlockedReceiverNoValueLost(t *testing.T) {
	// Closing one of two parked receivers must wake it without deadlocking
	// the sender or losing the value: either the closing receiver picks up
	// the value successfully (race won the channel arm at the instant of
	// close) and the still-open receiver gets nothing, or it wakes with
	// ErrClosed and the value is delivered to the still-open receiver.
	for i := 0; i < 1000; i++ {
		h, tx := newHubTx[int](t, 0) // rendezvous
		rx1 := h.Receiver()
		rx2 := h.Receiver()

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
			require.Equalf(t, 42, r1.v, "iter %d: rx1 took value but it was not 42", i)
			// rx2 is still parked; close the hub to release it.
			h.Close()
			r2 := <-rx2Done
			require.ErrorIsf(t, r2.err, gochan.ErrClosed, "iter %d: rx2 should have seen ErrClosed after hub close", i)
			continue
		}
		require.ErrorIsf(t, r1.err, gochan.ErrClosed, "iter %d: rx1 unexpected error", i)
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
	// Same as above for RecvContext.
	for i := 0; i < 1000; i++ {
		h, tx := newHubTx[int](t, 0)
		rx1 := h.Receiver()
		rx2 := h.Receiver()

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
		h, tx := newHubTx[int](t, n)
		rx1 := h.Receiver()
		rx2 := h.Receiver()
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

		// Each receiver must see a strictly increasing subsequence of sends.
		for _, seq := range [][]int{got1, got2} {
			for k := 1; k < len(seq); k++ {
				require.Greaterf(t, seq[k], seq[k-1], "iter %d: out-of-order receive %v", i, seq)
			}
		}
		// Union covers exactly 1..n with no duplicates.
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

// recvOp names a blocking recv-style method on a *Receiver.
type recvOp struct {
	name string
	call func(*Receiver[int]) error
}

var recvOps = []recvOp{
	{"Recv", func(rx *Receiver[int]) error { _, err := rx.Recv(); return err }},
	{"RecvContext", func(rx *Receiver[int]) error { _, err := rx.RecvContext(context.Background()); return err }},
}

// runParkedRecvCloseRace spawns the recv op, waits until it has genuinely
// parked in the select, and only then fires the trigger.
//
// A WaitGroup signalled before the call cannot establish that: it proves
// only that the goroutine was scheduled, so a trigger landing first would
// be answered by the entry-guard and the test would pass having covered
// the fast path rather than the parked select it names.
//
// spmc's Recv delegates to the embedded mpmc receiver, so the parked
// goroutine carries the spmc frame as the caller — matching on it still
// means "an spmc Recv is parked".
func runParkedRecvCloseRace(t *testing.T, op recvOp, trigger func(*Hub[int], *Receiver[int])) {
	t.Helper()
	h, _ := newHubTx[int](t, 0)
	rx := newRx(t, h)
	done := make(chan error, 1)
	base := parked.Snapshot(parked.InSelect, "spmc.(*Receiver[...])."+op.name+"(")
	go func() { done <- op.call(rx) }()
	base.Wait(t, 1)
	trigger(h, rx)
	assert.ErrorIs(t, <-done, gochan.ErrClosed)
}

// TestRecvSelectWakesOnReceiverClose covers the <-rx.done.Done() select
// arm of both Recv and RecvContext.
func TestRecvSelectWakesOnReceiverClose(t *testing.T) {
	for _, op := range recvOps {
		t.Run(op.name, func(t *testing.T) {
			runParkedRecvCloseRace(t, op, func(_ *Hub[int], rx *Receiver[int]) { rx.Close() })
		})
	}
}

// TestRecvSelectWakesOnHubClose covers the <-s.dead.Done() select arm of
// both Recv and RecvContext.
func TestRecvSelectWakesOnHubClose(t *testing.T) {
	for _, op := range recvOps {
		t.Run(op.name, func(t *testing.T) {
			runParkedRecvCloseRace(t, op, func(h *Hub[int], _ *Receiver[int]) { h.Close() })
		})
	}
}

// TestRecvContextProbeSeesChannelClosed covers RecvContext's first
// (non-blocking) select arm "case v, ok := <-s.ch" with !ok — sender closed
// the channel before RecvContext was called, dead is still open.
func TestRecvContextProbeSeesChannelClosed(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	tx.Close()
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, gochan.ErrClosed)
}

// TestRecvContextSelectWakesOnSenderClose covers RecvContext's second
// (blocking) select arm "case v, ok := <-s.ch" with !ok — sender closes the
// channel while RecvContext is parked, dead is not fired. Uses a fresh
// hub per iteration with sender-close as the trigger; can't share
// runParkedRecvCloseRace because that helper exposes only (hub, rx) to
// the trigger callback.
func TestRecvContextSelectWakesOnSenderClose(t *testing.T) {
	h, tx := newHubTx[int](t, 0)
	rx := newRx(t, h)
	done := make(chan error, 1)
	base := parked.Snapshot(parked.InSelect, "spmc.(*Receiver[...]).RecvContext(")
	go func() {
		_, err := rx.RecvContext(context.Background())
		done <- err
	}()
	base.Wait(t, 1)
	tx.Close()
	assert.ErrorIs(t, <-done, gochan.ErrClosed)
}

// TestRecvContextValueArrivesDuringWait covers RecvContext's second-select
// "case v, ok := <-s.ch" arm — value arrives after the non-blocking probe
// missed, while the receiver is parked in the second select.
func TestRecvContextValueArrivesDuringWait(t *testing.T) {
	h, tx := newHubTx[int](t, 1)
	rx := newRx(t, h)
	type result struct {
		v   int
		err error
	}
	done := make(chan result, 1)
	base := parked.Snapshot(parked.InSelect, "spmc.(*Receiver[...]).RecvContext(")
	go func() {
		v, err := rx.RecvContext(context.Background())
		done <- result{v, err}
	}()
	base.Wait(t, 1) // probe has missed; committed to the select
	require.NoError(t, tx.Send(42))
	r := <-done
	require.NoError(t, r.err)
	assert.Equal(t, 42, r.v)
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
