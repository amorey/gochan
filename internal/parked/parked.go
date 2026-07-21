// Package parked provides a sleep-free way for tests to wait until
// goroutines have actually parked inside a given function.
//
// It exists because most of this module's blocking paths leave no counter
// behind to poll. broadcast and watch track parked receivers in `waiters`
// and can be observed directly; a sender blocked on a full buffer, an
// mpmc receiver, and a oneshot receiver cannot. For those, the runtime's
// own view of the goroutine is the only observable.
//
// The failure it prevents is specific. A WaitGroup or a `started` channel
// signalled just before the blocking call only proves the goroutine was
// scheduled, not that it reached the select. A test that fires its
// trigger on that signal races the call's own entry-time checks and
// non-blocking probes: if the trigger lands first, the fast path answers,
// the parked arm under test is never entered, and the assertion still
// passes. Green, while covering the opposite of what it claims.
//
// Waiting is identity-based, not count-based: [Snapshot] records the
// goroutines already matching before the one under test is spawned, and
// [Baseline.Wait] counts only new arrivals. Counting alone is not enough,
// because runtime.Stack dumps every goroutine in the test binary — one
// left parked in the same frame by an earlier test would satisfy a count
// of 1 on its own, letting the caller proceed before the goroutine under
// test had parked at all. That is the very race this package exists to
// remove, so a stale goroutine must be excluded rather than merely
// noticed.
//
// Not part of the public API.
package parked

import (
	"runtime"
	"strconv"
	"strings"
)

// TB is the slice of [testing.TB] this package needs. It is a local
// interface rather than testing.TB itself so that this package's own
// tests can drive the failure paths with a stub that records a Fatalf
// instead of aborting — testing.TB has an unexported method and cannot be
// implemented outside the testing package. *testing.T satisfies this.
//
// A stub Fatalf returns instead of aborting the goroutine, so every
// Fatalf below is followed by a return rather than relying on
// runtime.Goexit.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// maxYields bounds a wait. The frame argument encodes the runtime's
// stack-trace rendering, so it can go stale — a renamed method, a moved
// select, or a change in how generic type parameters print would all stop
// it matching. Bounded-and-failing turns that into a named assertion
// failure instead of a hang until the package timeout.
//
// The bound is on scheduler yields, not wall-clock: it is orders of
// magnitude more than the handful of turns a real park needs, so a loaded
// CI machine cannot trip it.
//
// A var, not a const, only so this package's own tests can shrink it and
// reach the exhaustion paths quickly. Nothing else writes it.
var maxYields = 100_000

// yieldsPerScan is how many yields [Baseline.Wait] takes between stack
// dumps, which is what keeps a failing wait cheap.
//
// runtime.Stack(_, true) stops the world and formats every goroutine in
// the binary, so its cost scales with goroutine count — measured at
// roughly a millisecond in a test binary holding a thousand of them.
// Scanning once per yield would spend the entire yield budget on those
// dumps: ~100k of them, tens of seconds to minutes before the Fatalf
// fires, which is the hang this bound exists to prevent.
//
// Yields stay cheap and plentiful for the scheduler; only the sampling is
// rationed. A real park is seen on the first scan or two, so spacing them
// costs detection nothing. [WaitCount] needs none of this: its closure
// poll is a mutex and an int.
const yieldsPerScan = 128

// Blocked-goroutine states, as the runtime names them in a stack dump.
// Callers name the one they expect rather than accepting any blocked
// state, so a call that parks somewhere other than intended fails instead
// of quietly counting.
//
// These are the bare state names, without the brackets the runtime prints
// them in: see [headerState] for why the surrounding text cannot be
// matched literally.
const (
	InSelect   = "select"       // parked in a multi-arm select
	InChanRecv = "chan receive" // parked on a bare <-ch
)

// headerState returns the goroutine state from a stack-dump block — the
// text between "[" and the first "," or "]" of its header line.
//
// Parsed rather than substring-matched because the runtime decorates the
// state once a goroutine has been blocked a while: "[select]" becomes
// "[select, 2 minutes]", and a thread-locked goroutine reads
// "[chan receive, locked to thread]". Matching the bracketed literal
// would stop counting a goroutine at the one-minute mark — silently
// disabling [Wait]'s over-count guard for precisely the aged, leaked
// goroutine it exists to catch, since a fresh one under test would still
// match and satisfy the wait alone.
//
// Returns "" for a block with no bracketed state, which matches no
// caller-supplied state and so is not counted.
func headerState(block string) string {
	line, _, _ := strings.Cut(block, "\n")
	_, inside, ok := strings.Cut(line, "[")
	if !ok {
		return ""
	}
	state, _, _ := strings.Cut(inside, "]")
	state, _, _ = strings.Cut(state, ",")
	return state
}

// WaitCount blocks until count() reports at least want, yielding between
// polls. Prefer it over [Wait] wherever the implementation already tracks
// parked goroutines: it observes the thing itself rather than inferring
// it from a stack-trace rendering, so it cannot go stale.
//
// count is called without any lock held by this package; a caller whose
// counter lives under a mutex takes it inside the closure. what names
// what is being waited for, and appears in the failure message.
func WaitCount(t TB, what string, want int, count func() int) {
	t.Helper()
	for i := 0; i < maxYields; i++ {
		if count() >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("after %d yields, %s reached only %d of %d; "+
		"if the code under test is correct, that counter is no longer being maintained",
		maxYields, what, count(), want)
}

// Baseline is the set of goroutines already parked in a given state and
// frame at the moment it was taken. Obtain one from [Snapshot].
type Baseline struct {
	state, frame string
	notFrames    []string
	ids          map[uint64]bool
}

// Snapshot records which goroutines already match state and frame. Take
// it *before* spawning the goroutine under test — anything it captures is
// excluded from [Baseline.Wait], so a goroutine leaked by an earlier test
// can neither satisfy the wait nor trip its over-count check.
//
// frame is matched as a substring of the goroutine's stack, so it should
// include the trailing "(" to keep a method name from matching a longer
// one — "Recv(" does not match "RecvContext(". Callers are responsible
// for picking a frame in which the only reachable block is the one they
// mean to catch; a probe select carrying a default never parks, so it
// does not need excluding.
//
// notFrames excludes goroutines whose stack also contains any of them,
// which is what makes a delegating method distinguishable from the one
// it delegates to. A goroutine parked via mpmc's Recv carries both
// Recv( and RecvContext( on its stack, so a snapshot of RecvContext(
// alone would match it too; naming Recv( here narrows the match to
// direct RecvContext callers. Without that, a test spawning one of each
// after the snapshot would trip Wait's over-count check with a message
// pointing nowhere near the cause.
func Snapshot(state, frame string, notFrames ...string) *Baseline {
	buf := make([]byte, 1<<16)
	b := &Baseline{state: state, frame: frame, notFrames: notFrames}
	b.ids = b.scan(&buf)
	return b
}

// Wait blocks until exactly want goroutines beyond the baseline are
// parked in its state and frame, then returns. It fails the test if that
// does not happen within the yield budget, and fails immediately if it
// finds more than want — an over-count means the test spawned more than
// it declared, since anything predating the snapshot is already excluded.
func (b *Baseline) Wait(t TB, want int) {
	t.Helper()
	buf := make([]byte, 1<<16)
	for i := 0; i < maxYields; i++ {
		if i%yieldsPerScan == 0 {
			found := 0
			for id := range b.scan(&buf) {
				if !b.ids[id] {
					found++
				}
			}
			switch {
			case found == want:
				return
			case found > want:
				t.Fatalf("found %d new goroutine(s) in state %q under frame %q, want exactly %d; "+
					"if two frames here overlap — a method delegating to another — "+
					"exclude the outer one when taking the snapshot",
					found, b.state, b.frame, want)
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("after %d yields, fewer than %d new goroutine(s) in state %q under frame %q; "+
		"if the code under test is correct, that state or frame no longer matches",
		maxYields, want, b.state, b.frame)
}

// scan returns the ids of goroutines currently in b.state with b.frame
// on their stack and none of b.notFrames, growing buf until the whole
// dump fits.
func (b *Baseline) scan(buf *[]byte) map[uint64]bool {
	n := runtime.Stack(*buf, true)
	for n == len(*buf) {
		*buf = make([]byte, 2*len(*buf))
		n = runtime.Stack(*buf, true)
	}
	ids := make(map[uint64]bool)
	for _, g := range strings.Split(string((*buf)[:n]), "\n\ngoroutine ") {
		if headerState(g) != b.state || !strings.Contains(g, b.frame) {
			continue
		}
		if b.excluded(g) {
			continue
		}
		if id, ok := headerID(g); ok {
			ids[id] = true
		}
	}
	return ids
}

// headerID returns the goroutine number from a stack-dump block. Only the
// first block carries the literal "goroutine " prefix; the rest lose it to
// the split separator.
func headerID(block string) (uint64, bool) {
	line, _, _ := strings.Cut(block, "\n")
	num, _, _ := strings.Cut(strings.TrimPrefix(line, "goroutine "), " ")
	id, err := strconv.ParseUint(num, 10, 64)
	return id, err == nil
}

// excluded reports whether this goroutine's stack carries any frame the
// baseline was told to reject.
func (b *Baseline) excluded(block string) bool {
	for _, f := range b.notFrames {
		if strings.Contains(block, f) {
			return true
		}
	}
	return false
}
