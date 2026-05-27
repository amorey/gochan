package watch

// SetFeederParkedHook installs a test-only callback invoked by the
// Chan feeder goroutine each time it snapshots a value and enters
// the send select. Available only to tests in this package via
// build-time access to unexported fields.
func (rx *Receiver[T]) SetFeederParkedHook(fn func()) { rx.testFeederParked = fn }

// ReceiverCount returns the number of receivers currently registered
// with the hub. Exposed for tests that verify deregistration on
// terminal ErrClosed.
func (h *Hub[T]) ReceiverCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.receivers)
}
