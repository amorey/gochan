package chancore

import (
	"context"

	"github.com/amorey/gochan"
)

// BufferedRecv is the shared receive-side core for chan-backed pipelines.
// It pairs a value channel with a termination ("dead") signal: the dead
// signal is preferred over buffered values, and a closed value channel
// yields ErrClosed. Ready, if non-nil, is a one-shot readiness latch
// (e.g. "at least one sender has registered"); when it has not yet
// fired, TryRecv reports ErrNotReady instead of ErrEmpty so callers can
// distinguish "buffer is empty but a sender is producing" from "nobody
// is producing yet". Recv and RecvContext ignore Ready — they block
// naturally until a value arrives or Dead fires.
//
// The struct is plain data; it has no internal locking. Multiple receiver
// handles in the same package can share one BufferedSend/BufferedRecv pair
// (e.g. spmc), and per-handle close state is layered above by the wrapper.
type BufferedRecv[T any] struct {
	Ch    <-chan T
	Dead  <-chan struct{}
	Ready *CloseOnce // nil = no readiness gate
}

// Recv blocks until a value is available, the value channel is closed, or
// Dead fires.
func (r *BufferedRecv[T]) Recv() (T, error) {
	var z T
	select {
	case <-r.Dead:
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-r.Ch:
		if !ok {
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-r.Dead:
		return z, gochan.ErrClosed
	}
}

// TryRecv is the non-blocking variant of Recv.
func (r *BufferedRecv[T]) TryRecv() (T, error) {
	var z T
	select {
	case <-r.Dead:
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-r.Ch:
		if !ok {
			return z, gochan.ErrClosed
		}
		return v, nil
	default:
	}
	if r.Ready != nil && !r.Ready.IsClosed() {
		return z, gochan.ErrNotReady
	}
	return z, gochan.ErrEmpty
}

// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled
// first. A ready value is preferred over a cancelled context; Dead is
// preferred over both.
func (r *BufferedRecv[T]) RecvContext(ctx context.Context) (T, error) {
	var z T
	select {
	case <-r.Dead:
		return z, gochan.ErrClosed
	default:
	}
	select {
	case v, ok := <-r.Ch:
		if !ok {
			return z, gochan.ErrClosed
		}
		return v, nil
	default:
	}
	select {
	case v, ok := <-r.Ch:
		if !ok {
			return z, gochan.ErrClosed
		}
		return v, nil
	case <-r.Dead:
		return z, gochan.ErrClosed
	case <-ctx.Done():
		return z, ctx.Err()
	}
}
