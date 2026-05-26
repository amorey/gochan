// Package gochan provides specialized channel architectures inspired by Rust.
//
// Sentinel errors defined here are shared across all subpackages.
package gochan

import "errors"

var (
	ErrClosed = errors.New("chans: channel closed")
	ErrFull   = errors.New("chans: channel full")
	ErrEmpty  = errors.New("chans: channel empty")
)
