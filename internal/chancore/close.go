// Package chancore holds shared building blocks for the gochan
// subpackages. It is internal; nothing here is part of the public API.
package chancore

import "sync/atomic"

// CloseOnce is a one-shot termination signal. The done channel is closed
// exactly once; concurrent callers safely race on Close and only the
// winner performs the close.
type CloseOnce struct {
	flag atomic.Bool
	ch   chan struct{}
}

// NewCloseOnce returns a CloseOnce whose done channel is not yet closed.
func NewCloseOnce() *CloseOnce {
	c := &CloseOnce{}
	c.Init()
	return c
}

// Init prepares an embedded CloseOnce for use. Call once on a
// zero-valued CloseOnce before any other method. Useful when the
// CloseOnce lives as a value field inside another struct, saving
// the separate heap allocation that [NewCloseOnce] would incur.
func (c *CloseOnce) Init() { c.ch = make(chan struct{}) }

// Close closes the done channel iff this is the first successful call.
// Returns true if this call performed the close.
func (c *CloseOnce) Close() bool {
	if c.flag.CompareAndSwap(false, true) {
		close(c.ch)
		return true
	}
	return false
}

// Done returns the channel that is closed by Close.
func (c *CloseOnce) Done() <-chan struct{} { return c.ch }

// IsClosed reports whether Close has been called.
func (c *CloseOnce) IsClosed() bool { return c.flag.Load() }
