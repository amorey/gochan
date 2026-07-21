# Project Guidance

## Overview

`gochan` (module `github.com/amorey/gochan`) is a small Go library that provides
seven specialized channel architectures beyond Go's built-in mpmc `chan`:
`oneshot`, `spsc`, `spmc`, `mpsc`, `mpmc`, `broadcast`, and `watch`. Requires
Go 1.21+.

See `README.md` for the full public API reference (constructors, methods,
semantics tables, error meanings). When changing public behavior, update
`README.md` in the same change.

## Layout

- `gochan.go` — common `Sender[T]` and `Receiver[T]` interfaces every
  package's handles implement. There is intentionally no shared `Hub`
  interface — each multi-side package exposes its own concrete `*Hub[T]`
  so callers can't accidentally substitute one architecture for another.
- `errors.go` — shared sentinel errors: `ErrClosed`, `ErrFull`, `ErrEmpty`,
  `ErrNotReady`, and `ErrLagged` (broadcast only).
- `oneshot/`, `spsc/` — singleton-pair packages. Constructors return
  `(*Sender[T], *Receiver[T])`; each side is closed via its own
  `Close()`. There is intentionally no close-all aggregator — there is
  no hub to hang one on, and callers typically own each side from a
  different goroutine.
- `spmc/`, `mpsc/`, `mpmc/`, `broadcast/`, `watch/` — multi-side packages.
  Constructors return `*Hub[T]`; handles are minted via `hub.Sender()` /
  `hub.Receiver()` and `hub.Close()` is close-all (hard tear-down across
  all packages). For `watch`, use `Sender.Close()` if you want subscribers
  to observe the final value once before shutdown — `Hub.Close()` skips
  that drain.
- `internal/chancore/` — shared building blocks for the chan-backed
  packages. `mpmc` is the only direct consumer; `spmc` and `mpsc` wrap
  `mpmc`, and `spsc` wraps `mpsc`, so all four share this core. Not part
  of the public API. (`broadcast` and `watch` use only `CloseOnce`.)
  - `CloseOnce` — one-shot termination signal (atomic flag + done channel).
  - `BufferedSend[T]` — shared send core that handles the value channel, a
    `Dead` termination signal, a required `Ready` readiness latch (drives
    `ErrNotReady` on `TrySend` before any counterparty has registered), and
    a `ChClosed` atomic fast-path so senders never write to a closed
    channel. There is no matching recv core: the receiver needs a
    per-handle close arm, so `mpmc`'s receive loops live in `mpmc` itself.
    `Dead` and `Ready` are both required — a send-side with no readiness
    condition passes an already-latched `CloseOnce` rather than nil.

  Prefer extending `chancore` over duplicating select/close logic across
  packages.

## Conventions

- Bounded by default: every queue-style constructor takes an explicit
  capacity. `broadcast.New[T](0)` panics; the other queue constructors
  accept `0` as rendezvous and panic on negative capacity.
- Handle `Close()` is always idempotent. Close-all is equivalent to calling
  `Close()` on every handle the hub has handed out, across every multi-side
  package. In `watch`, `Hub.Close()` is therefore hard tear-down (sender +
  every live receiver, no final-value drain); call `Sender.Close()` instead
  if you want the soft drain pattern where receivers observe the latest
  value once before ErrClosed.
- `Receiver.Chan()` close behavior depends on the package family:
  - Queue-style (`spsc`, `spmc`, `mpsc`, `mpmc`): the channel is the shared
    buffer; `Receiver.Close()` does *not* close it. It only closes when
    the sender closes (directly or via close-all) and the buffer drains,
    mirroring `close()` on a raw Go channel.
  - Per-receiver feeder (`broadcast`, `watch`): the channel is private to
    the receiver and driven by a per-receiver feeder goroutine.
    `Receiver.Close()` *does* close it (the feeder shuts down), and
    sender-close also closes it after the feeder drains its last value.
- Don't call close-all concurrently with an active `Send` from a different
  goroutine — it inherits the sender's close discipline.

## Testing

- Use [testify](https://github.com/stretchr/testify) (`assert` and `require`) for assertions in tests.
- Do not use magic sleeps (`time.Sleep`, or `time.After` timeouts whose duration encodes an assumption about scheduling) to coordinate goroutines or "wait for" state changes. Synchronize through channels or observable state instead. `context.WithTimeout` is fine when the timeout itself is the thing under test.
- Run `go test ./...` from the repo root; each package has its own `*_test.go` alongside the implementation.
