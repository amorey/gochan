package gochan

import "context"

// Sender is the common send-side interface implemented by every channel type
// in this module.
type Sender[T any] interface {
	// Send blocks until the value is delivered or the channel is closed.
	Send(v T) error
	// TrySend returns immediately without blocking. Returns nil on
	// success, or one of: ErrFull (no room to enqueue), ErrClosed
	// (sender/hub closed), ErrNotReady (queue-style packages — no
	// counterparty has registered yet).
	TrySend(v T) error
	// SendContext blocks like Send but returns ctx.Err() if ctx is cancelled.
	SendContext(ctx context.Context, v T) error
	// Close is idempotent.
	Close()
}

// Receiver is the common receive-side interface implemented by every channel
// type in this module.
type Receiver[T any] interface {
	// Recv blocks until a value is received or the channel is closed.
	Recv() (T, error)
	// TryRecv returns immediately without blocking. Returns the next
	// value, or one of: ErrEmpty (nothing buffered), ErrClosed
	// (sender/hub closed and nothing left to drain), ErrNotReady
	// (queue-style packages — no counterparty has registered yet),
	// ErrLagged (broadcast — receiver fell behind, see package docs).
	TryRecv() (T, error)
	// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled.
	RecvContext(ctx context.Context) (T, error)
	// Chan returns a native channel for use with select.
	Chan() <-chan T
	// Close is idempotent.
	Close()
}
