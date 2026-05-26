package broadcast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/gochan"
)

// TestEOFUnregistersReceiver verifies that draining to ErrClosed after
// Sender.Close unregisters the receiver and releases its ring payloads,
// without requiring an explicit Receiver.Close call.
func TestEOFUnregistersReceiver(t *testing.T) {
	h := New[*int](2)
	rx := h.Receiver()
	tx := h.Sender()

	val := 11
	require.NoError(t, tx.Send(&val))
	require.NoError(t, tx.Send(&val))
	tx.Close()

	// Drain via Recv until ErrClosed — the natural EOF path.
	for {
		_, err := rx.Recv()
		if err != nil {
			require.ErrorIs(t, err, gochan.ErrClosed)
			break
		}
	}

	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	assert.Empty(t, h.s.receivers, "EOF Recv should unregister the receiver")
	for i, slot := range h.s.buf {
		assert.Nilf(t, slot, "ring slot %d should be cleared after last receiver leaves", i)
	}
}

// TestTryRecvEOFUnregistersReceiver covers the TryRecv variant of the
// same path: a caller that polls with TryRecv and observes ErrClosed
// must also unregister.
func TestTryRecvEOFUnregistersReceiver(t *testing.T) {
	h := New[*int](2)
	rx := h.Receiver()
	tx := h.Sender()

	val := 13
	require.NoError(t, tx.Send(&val))
	tx.Close()

	for {
		_, err := rx.TryRecv()
		if err == gochan.ErrEmpty {
			t.Fatal("unexpected ErrEmpty after sender close")
		}
		if err == nil {
			continue
		}
		require.ErrorIs(t, err, gochan.ErrClosed)
		break
	}

	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	assert.Empty(t, h.s.receivers, "EOF TryRecv should unregister the receiver")
	for i, slot := range h.s.buf {
		assert.Nilf(t, slot, "ring slot %d should be cleared after last receiver leaves", i)
	}
}
