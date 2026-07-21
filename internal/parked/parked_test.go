package parked

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubTB records Fatalf instead of aborting, which is what makes the
// failure paths below testable at all: a real *testing.T would fail the
// very test that is asserting the failure.
type stubTB struct {
	helpers int
	fatals  []string
}

func (s *stubTB) Helper() { s.helpers++ }
func (s *stubTB) Fatalf(format string, args ...any) {
	s.fatals = append(s.fatals, fmt.Sprintf(format, args...))
}
func (s *stubTB) only(t *testing.T) string {
	t.Helper()
	require.Len(t, s.fatals, 1, "expected exactly one Fatalf")
	return s.fatals[0]
}

// shrinkYields makes the exhaustion paths reachable in microseconds
// instead of the ~3s a full budget takes.
func shrinkYields(t *testing.T) {
	t.Helper()
	orig := maxYields
	maxYields = 20
	t.Cleanup(func() { maxYields = orig })
}

// blockForTest parks on a bare channel receive so Wait has a frame to
// match. Named rather than a closure so the frame is stable.
func blockForTest(ch chan struct{}) { <-ch }

// blockViaWrapper reaches the same park through an extra frame, standing
// in for a method that delegates to another (mpmc's Recv -> RecvContext).
func blockViaWrapper(ch chan struct{}) { blockForTest(ch) }

const (
	blockFrame   = "parked.blockForTest("
	wrapperFrame = "parked.blockViaWrapper("
)

// park spawns n goroutines in blockForTest and returns a release func.
// Every test releases its goroutines: a leaked one would be counted by
// the next test's Wait, which is the exact hazard Wait's exact-count
// check exists to report.
func park(t *testing.T, n int) *Baseline {
	t.Helper()
	base := Snapshot(InChanRecv, blockFrame)
	ch := make(chan struct{})
	for i := 0; i < n; i++ {
		go blockForTest(ch)
	}
	t.Cleanup(func() { close(ch) })
	return base
}

func TestWaitReturnsOnExactCount(t *testing.T) {
	base := park(t, 2)
	var stub stubTB
	base.Wait(&stub, 2)
	assert.Empty(t, stub.fatals)
	assert.Positive(t, stub.helpers, "Helper not called")
}

// A goroutine already parked in the frame when the snapshot is taken is
// excluded, so it can neither satisfy a later wait nor trip its
// over-count check. This is the guarantee counting alone cannot give: a
// stale goroutine would otherwise answer a want of 1 by itself, before
// the goroutine under test had parked.
func TestBaselineExcludesPreexistingGoroutines(t *testing.T) {
	stale := park(t, 1)
	var settle stubTB
	stale.Wait(&settle, 1)
	require.Empty(t, settle.fatals, "stale goroutine never parked")

	fresh := park(t, 1) // snapshot taken with the stale one already parked
	var stub stubTB
	fresh.Wait(&stub, 1)
	assert.Empty(t, stub.fatals, "baseline should have excluded the stale goroutine")
}

func TestWaitRejectsSurplusGoroutine(t *testing.T) {
	base := park(t, 2)
	// Establish that both are parked before asking for one. Without this
	// the wait races the second goroutine: seeing 1 of 2 is a legitimate
	// exact match, so it would return successfully and the surplus check
	// would go untested — the same "proves it was scheduled, not parked"
	// mistake this package exists to prevent.
	var settled stubTB
	base.Wait(&settled, 2)
	require.Empty(t, settled.fatals)

	var stub stubTB
	base.Wait(&stub, 1)
	assert.Contains(t, stub.only(t), "want exactly 1")
}

func TestWaitFailsWhenNothingParks(t *testing.T) {
	shrinkYields(t)
	var stub stubTB
	Snapshot(InChanRecv, blockFrame).Wait(&stub, 1)
	assert.Contains(t, stub.only(t), "no longer matches")
}

// A state mismatch must fail like a frame mismatch: the goroutines are
// parked in the frame, but on a channel receive rather than a select.
func TestWaitFailsOnStateMismatch(t *testing.T) {
	shrinkYields(t)
	base := Snapshot(InSelect, blockFrame)
	park(t, 1)
	var stub stubTB
	base.Wait(&stub, 1)
	assert.Contains(t, stub.only(t), "fewer than 1")
}

func TestWaitCountReturnsWhenReached(t *testing.T) {
	n := 0
	var stub stubTB
	WaitCount(&stub, "widgets", 2, func() int { n++; return n })
	assert.Empty(t, stub.fatals)
}

func TestWaitCountFailsWhenNeverReached(t *testing.T) {
	shrinkYields(t)
	var stub stubTB
	WaitCount(&stub, "widgets", 3, func() int { return 1 })
	msg := stub.only(t)
	assert.Contains(t, msg, "widgets reached only 1 of 3")
}

// The scan must survive a stack dump larger than the initial buffer, so
// the growth loop is not a latent truncation bug.
func TestWaitGrowsBufferForLargeDump(t *testing.T) {
	base := park(t, 400)
	var stub stubTB
	base.Wait(&stub, 400)
	assert.Empty(t, stub.fatals)
}

// TestHeaderStateIgnoresRuntimeDecorations pins the parse against the
// renderings the runtime actually emits. The aged form is the one that
// matters: a goroutine blocked past a minute prints ", 2 minutes", and
// matching the bracketed literal would stop counting it — disabling
// Wait's over-count guard for exactly the stale goroutine it exists to
// catch. Table-driven rather than timing-based because reproducing the
// aged form for real would mean blocking a goroutine for a minute.
func TestHeaderStateIgnoresRuntimeDecorations(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  string
	}{
		{"fresh", "goroutine 1 [select]:\nmain.f()", InSelect},
		{"aged", "goroutine 42 [select, 2 minutes]:\nmain.f()", InSelect},
		{"aged chan receive", "goroutine 7 [chan receive, 15 minutes]:\nmain.f()", InChanRecv},
		{"locked to thread", "goroutine 9 [chan receive, locked to thread]:\nmain.f()", InChanRecv},
		{"running", "goroutine 3 [running]:\nmain.f()", "running"},
		{"no bracket", "malformed block\nmain.f()", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, headerState(tc.block))
		})
	}
}

// A real dump must parse to the same state the constants name, so the
// table above cannot drift from what the runtime emits.
func TestHeaderStateMatchesRealDump(t *testing.T) {
	base := park(t, 1)
	var stub stubTB
	base.Wait(&stub, 1)
	require.Empty(t, stub.fatals)

	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	var seen bool
	for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
		if strings.Contains(g, blockFrame) {
			assert.Equal(t, InChanRecv, headerState(g))
			seen = true
		}
	}
	assert.True(t, seen, "no goroutine found in the test frame")
}

func TestHeaderID(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  uint64
		ok    bool
	}{
		{"first block keeps its prefix", "goroutine 1 [running]:\nmain.f()", 1, true},
		{"split block lost its prefix", "42 [select]:\nmain.f()", 42, true},
		{"not a number", "abc [select]:\nmain.f()", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := headerID(tc.block)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.want, id)
			}
		})
	}
}

// TestSnapshotNotFramesExcludesDelegators pins notFrames: a goroutine
// reaching the park through a wrapper carries both frames, so a snapshot
// of the inner frame alone would count it. Naming the outer frame narrows
// the match to direct callers — without which a test spawning one of each
// would trip Wait's over-count check for no real reason.
func TestSnapshotNotFramesExcludesDelegators(t *testing.T) {
	ch := make(chan struct{})
	t.Cleanup(func() { close(ch) })

	direct := Snapshot(InChanRecv, blockFrame, wrapperFrame)
	both := Snapshot(InChanRecv, blockFrame)
	go blockForTest(ch)
	go blockViaWrapper(ch)

	// Both goroutines park in blockForTest, so the unfiltered baseline
	// sees two; the filtered one sees only the direct caller.
	var stub stubTB
	both.Wait(&stub, 2)
	require.Empty(t, stub.fatals, "both goroutines should have parked")

	var filtered stubTB
	direct.Wait(&filtered, 1)
	assert.Empty(t, filtered.fatals, "wrapper-reached goroutine should be excluded")
}
