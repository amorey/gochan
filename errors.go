// Package gochan provides specialized channel architectures inspired by Rust.
//
// Sentinel errors defined here are shared across all subpackages.
package gochan

import "errors"

var (
	ErrClosed = errors.New("gochan: channel closed")
	ErrFull   = errors.New("gochan: channel full")
	ErrEmpty  = errors.New("gochan: channel empty")
)
