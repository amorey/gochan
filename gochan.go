package gochan

import "context"

// Hub is the construction-handle interface implemented by multi-side
// channel packages in this module. Each subpackage's *Hub[T] satisfies
// it.
type Hub[T any] interface {
	// Sender hands out a send-side handle. On singleton-Sender packages
	// it returns the same handle on every call; on multi-Sender packages
	// it returns a fresh handle each time. After the hub is closed the
	// returned handle reports ErrClosed on use.
	Sender() Sender[T]
	// Receiver hands out a receive-side handle. On singleton-Receiver
	// packages it returns the same handle on every call; on
	// multi-Receiver packages it returns a fresh handle each time.
	// After the hub is closed the returned handle reports ErrClosed on
	// use.
	Receiver() Receiver[T]
	// Close is the hub-wide kill-switch. Idempotent.
	Close()
}

// Sender is the common send-side interface implemented by every channel type
// in this module.
type Sender[T any] interface {
	// Send blocks until the value is delivered or the channel is closed.
	Send(v T) error
	// TrySend returns ErrFull or ErrClosed immediately without blocking.
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
	// TryRecv returns ErrEmpty or ErrClosed immediately without blocking.
	TryRecv() (T, error)
	// RecvContext blocks like Recv but returns ctx.Err() if ctx is cancelled.
	RecvContext(ctx context.Context) (T, error)
	// Chan returns a native channel for use with select.
	Chan() <-chan T
	// Close is idempotent.
	Close()
}
