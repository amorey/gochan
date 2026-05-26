package watch

// SetFeederParkedHook installs a test-only callback invoked by the
// Chan feeder goroutine each time it snapshots a value and enters
// the send select. Available only to tests in this package via
// build-time access to unexported fields.
func (rx *Receiver[T]) SetFeederParkedHook(fn func()) { rx.testFeederParked = fn }
