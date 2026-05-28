package watch

// forTestingReceiverCount returns the number of receivers currently
// registered with the hub. Tests use it to verify deregistration on
// terminal ErrClosed.
func (h *Hub[T]) forTestingReceiverCount() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.receivers)
}
