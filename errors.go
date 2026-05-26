// Package gochan provides specialized channel architectures inspired by Rust.
//
// Sentinel errors defined here are shared across all subpackages.
package gochan

import (
	"errors"
	"fmt"
)

var (
	ErrClosed   = errors.New("gochan: channel closed")
	ErrFull     = errors.New("gochan: channel full")
	ErrEmpty    = errors.New("gochan: channel empty")
	ErrNotReady = errors.New("gochan: no counterparty registered")
)

// ErrLagged is returned by broadcast receivers that have fallen behind
// the ring buffer. Missed reports how many values were overwritten
// before the receiver caught up; the receiver's read position is
// advanced to the oldest still-buffered value and remains usable.
type ErrLagged struct{ Missed uint64 }

func (e ErrLagged) Error() string {
	return fmt.Sprintf("gochan: receiver lagged, missed %d values", e.Missed)
}
