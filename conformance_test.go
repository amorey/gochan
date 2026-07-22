package gochan_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/broadcast"
	"github.com/amorey/gochan/mpmc"
	"github.com/amorey/gochan/mpsc"
	"github.com/amorey/gochan/oneshot"
	"github.com/amorey/gochan/spmc"
	"github.com/amorey/gochan/spsc"
	"github.com/amorey/gochan/watch"
)

// This file lives in package gochan_test (not gochan) because it imports
// every sub-package, and those import the root for the sentinel errors.

// architecture is one channel package under conformance test: a name and a
// constructor handing back the two handles as the module-wide interfaces.
type architecture struct {
	name string
	// newPair returns a fresh, live sender/receiver pair. For hub-style
	// packages it mints one of each from a fresh hub.
	newPair func() (gochan.Sender[int], gochan.Receiver[int])
}

// architectures covers all seven packages. Capacity 1 is enough for every
// case below: none of them needs more than one value in flight.
var architectures = []architecture{
	{"oneshot", func() (gochan.Sender[int], gochan.Receiver[int]) {
		tx, rx := oneshot.New[int]()
		return tx, rx
	}},
	{"spsc", func() (gochan.Sender[int], gochan.Receiver[int]) {
		tx, rx := spsc.New[int](1)
		return tx, rx
	}},
	{"spmc", func() (gochan.Sender[int], gochan.Receiver[int]) {
		h := spmc.New[int](1)
		return h.Sender(), h.Receiver()
	}},
	{"mpsc", func() (gochan.Sender[int], gochan.Receiver[int]) {
		h := mpsc.New[int](1)
		return h.Sender(), h.Receiver()
	}},
	{"mpmc", func() (gochan.Sender[int], gochan.Receiver[int]) {
		h := mpmc.New[int](1)
		return h.Sender(), h.Receiver()
	}},
	{"broadcast", func() (gochan.Sender[int], gochan.Receiver[int]) {
		h := broadcast.New[int](1)
		return h.Sender(), h.Receiver()
	}},
	{"watch", func() (gochan.Sender[int], gochan.Receiver[int]) {
		h := watch.New(0)
		return h.Sender(), h.Receiver()
	}},
}

// TestRecvContextPrecedenceConformance walks every package through the same
// three-case matrix, pinning the closed > cancelled > value precedence the
// README advertises as uniform across the module.
//
// The rule is open-coded once per package — there is no shared recv core to
// hang it on, since each family needs its own termination arm — so a package
// that drops one of the arms is otherwise invisible: its own package tests
// pass and nothing compares it against its siblings. This table is what makes
// the omission fail somewhere.
//
// Only receiver-close is used for the "closed" arm. Sender-close is
// deliberately excluded: on the fan-in packages one sender closing does not
// terminate a hub that may still have other live senders, so it is not a
// module-wide terminal state the way receiver-close is.
func TestRecvContextPrecedenceConformance(t *testing.T) {
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			// closed > cancelled: a receiver already closed on entry
			// reports ErrClosed even for an already-cancelled ctx, since
			// that is the durable answer no retry could change.
			t.Run("closed beats cancelled", func(t *testing.T) {
				_, rx := a.newPair()
				rx.Close()
				_, err := rx.RecvContext(cancelled())
				assert.ErrorIs(t, err, gochan.ErrClosed)
			})

			// Same, reached the other way: every sender closed and nothing
			// left to drain is just as durably terminal as receiver-close,
			// so it must outrank a cancelled ctx too. A shutdown loop that
			// cancels its ctx and then drains to ErrClosed depends on this.
			t.Run("drained sender-close beats cancelled", func(t *testing.T) {
				tx, rx := a.newPair()
				// Drain first so "nothing left" actually holds. It is not
				// vacuous for every package: a fresh watch receiver starts
				// with the hub's initial value already pending, and with a
				// value in hand the correct answer is ctx.Err(), not
				// ErrClosed. TryRecv is the uniform way to reach the
				// drained state — everywhere else it errors immediately.
				for {
					if _, err := rx.TryRecv(); err != nil {
						break
					}
				}
				tx.Close()
				_, err := rx.RecvContext(cancelled())
				assert.ErrorIs(t, err, gochan.ErrClosed)
			})

			// cancelled > value: a live receiver with a value ready still
			// reports ctx.Err(), and leaves the value for the next Recv
			// rather than consuming and discarding it. Without this a
			// worker looping on RecvContext against a fast producer never
			// observes its own shutdown signal.
			t.Run("cancelled beats value", func(t *testing.T) {
				tx, rx := a.newPair()
				require.NoError(t, tx.Send(42))

				_, err := rx.RecvContext(cancelled())
				require.ErrorIs(t, err, context.Canceled)

				v, err := rx.Recv()
				require.NoError(t, err)
				assert.Equal(t, 42, v)
			})

			// value: nothing terminal, nothing cancelled — the ready value
			// wins. Guards against a probe ordering that makes either check
			// above swallow the normal path.
			t.Run("value on live ctx", func(t *testing.T) {
				tx, rx := a.newPair()
				require.NoError(t, tx.Send(42))

				v, err := rx.RecvContext(context.Background())
				require.NoError(t, err)
				assert.Equal(t, 42, v)
			})
		})
	}
}

// TestSenderCloseDrainsBeforeClosedConformance pins the one case where a
// close does *not* outrank a pending value: sender-close is a graceful
// end-of-stream, so an already-sent value is delivered first and ErrClosed
// only follows once nothing is left.
//
// This is the counterpart to the receiver-close arm of
// TestRecvContextPrecedenceConformance, which that table deliberately
// excludes — see its comment. The two directions are easy to conflate, and
// getting them backwards is invisible per-package: a package that let
// sender-close pre-empt the value would still look internally consistent.
// Here it fails against its siblings.
//
// Uniform across all seven packages, by three different mechanisms: the
// chan-backed four close the value channel (so the buffer drains first),
// broadcast/watch gate their terminal check on the receiver having caught
// up, and oneshot's Close is a no-op once the slot holds a value.
func TestSenderCloseDrainsBeforeClosedConformance(t *testing.T) {
	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			tx, rx := a.newPair()
			require.NoError(t, tx.Send(42))
			tx.Close()

			// The value survives the close that raced it.
			v, err := rx.Recv()
			require.NoError(t, err, "sender-close pre-empted a pending value")
			assert.Equal(t, 42, v)

			// ...and only then is the stream terminal.
			_, err = rx.Recv()
			assert.ErrorIs(t, err, gochan.ErrClosed)
		})
	}
}

// TestSendContextPrecedenceConformance is the send-side twin: closed >
// cancelled. There is no third rank — a send has no ready value competing
// with the cancellation — but the first step must hold everywhere the
// README claims it does.
func TestSendContextPrecedenceConformance(t *testing.T) {
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			// closed > cancelled: a sender already closed on entry reports
			// ErrClosed even for an already-cancelled ctx, because that is
			// the durable answer — a retry with a fresh context would only
			// return it again.
			t.Run("closed beats cancelled", func(t *testing.T) {
				tx, _ := a.newPair()
				tx.Close()
				assert.ErrorIs(t, tx.SendContext(cancelled(), 1), gochan.ErrClosed)
			})

			// cancelled on a live sender still reports ctx.Err().
			t.Run("cancelled on live sender", func(t *testing.T) {
				tx, _ := a.newPair()
				assert.ErrorIs(t, tx.SendContext(cancelled(), 1), context.Canceled)
			})
		})
	}
}

// TestTryRecvFlushTerminatesOnAnyError pins the flush loop the README
// prescribes for draining after a cancellation. The stopping condition is
// "any error", not ErrEmpty: which error ends the loop depends on whether
// the sender is still open, and a loop waiting for ErrEmpty alone would
// spin forever against a closed one — the ordinary shutdown case.
func TestTryRecvFlushTerminatesOnAnyError(t *testing.T) {
	for _, a := range architectures {
		t.Run(a.name, func(t *testing.T) {
			for _, closeSender := range []bool{false, true} {
				name := "sender open"
				if closeSender {
					name = "sender closed"
				}
				t.Run(name, func(t *testing.T) {
					tx, rx := a.newPair()
					require.NoError(t, tx.Send(42))
					if closeSender {
						tx.Close()
					}
					// Bounded so a non-terminating loop fails instead of
					// hanging the suite.
					var err error
					for i := 0; i < 100; i++ {
						if _, err = rx.TryRecv(); err != nil {
							break
						}
					}
					require.Error(t, err, "flush loop never reached a terminal error")
					if closeSender {
						assert.ErrorIs(t, err, gochan.ErrClosed,
							"a closed sender ends the flush with ErrClosed, never ErrEmpty")
					}
				})
			}
		})
	}
}
