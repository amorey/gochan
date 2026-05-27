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
  `(*Sender[T], *Receiver[T], func())`; the `func()` is close-all.
- `spmc/`, `mpsc/`, `mpmc/`, `broadcast/`, `watch/` — multi-side packages.
  Constructors return `*Hub[T]`; handles are minted via `hub.Sender()` /
  `hub.Receiver()` and `hub.Close()` is close-all (except `watch.Hub.Close`,
  which closes only the sender — see Conventions below).
- `internal/chancore/` — shared building blocks used by the chan-backed
  packages (`spsc`, `spmc`, `mpsc`, `mpmc`). Not part of the public API.
  - `CloseOnce` — one-shot termination signal (atomic flag + done channel).
  - `BufferedSend[T]` / `BufferedRecv[T]` — shared send/recv cores that
    handle the value channel, a `Dead` termination signal, an optional
    `Ready` readiness latch (drives `ErrNotReady` on `TrySend`/`TryRecv`
    before any counterparty has registered), and a `ChClosed` atomic
    fast-path so senders never write to a closed channel.

  Prefer extending `chancore` over duplicating select/close logic across
  packages.

## Conventions

- Bounded by default: every queue-style constructor takes an explicit
  capacity. `broadcast.New[T](0)` panics; the other queue constructors
  accept `0` as rendezvous and panic on negative capacity.
- Handle `Close()` is always idempotent. Close-all is equivalent to calling
  `Close()` on every handle the hub has handed out, with one exception:
  `watch.Hub.Close()` only closes the sender so live receivers can still
  observe the final published value.
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
