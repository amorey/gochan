// Package spsc provides a single-producer, single-consumer FIFO queue.
//
// [New] hands out a sender, a receiver, and a close function that
// calls Close on both. Values flow in order from sender to receiver.
// Capacity behaves exactly like a Go buffered channel: New[T](0)
// is a rendezvous channel, New[T](n) allows n queued values before
// Send blocks.
//
// Exactly one goroutine should call Send/Close on the sender, and exactly
// one goroutine should call Recv/Close on the receiver. The close
// function inherits the sender's close discipline — don't call it
// concurrently with an active Send from a different goroutine.
//
// Implementation note: spsc is a thin wrapper around [mpsc] with exactly
// one sender pre-registered, so the two packages share the underlying
// queue, close-coordination, and select machinery.
package spsc

import "github.com/amorey/gochan/mpsc"

// Sender is the send-side handle of an spsc pair.
type Sender[T any] struct{ *mpsc.Sender[T] }

// Receiver is the receive-side handle of an spsc pair.
type Receiver[T any] struct{ *mpsc.Receiver[T] }

// New creates a fresh spsc pair backed by a buffered Go channel of
// the given capacity, returning a sender, a receiver, and a close
// function that calls Close on both (Receiver first, then the underlying
// channel, so an in-flight Send escapes via the close signal before the
// channel is closed). capacity == 0 yields a rendezvous channel where
// Send blocks until a matching Recv is ready. capacity < 0 panics. The
// close function is idempotent and safe to defer.
func New[T any](capacity int) (*Sender[T], *Receiver[T], func()) {
	if capacity < 0 {
		panic("spsc: negative capacity")
	}
	h := mpsc.New[T](capacity)
	tx := h.Sender().(*mpsc.Sender[T])
	rx := h.Receiver().(*mpsc.Receiver[T])
	return &Sender[T]{tx}, &Receiver[T]{rx}, h.Close
}
