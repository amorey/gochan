# gochan

*gochan is a small library that implements multiple channel architectures for Go, inspired by Rust*

## Introduction

Go channels are extremely useful but they only ship with one type - mpmc (multiple-producer/multiple-consumer), buffered or un-buffered. This means that we often have to add higher level logic to our data structures in order to implement common patterns like single-shot, broadcasts and watches. Inspired by [`Rust channels`](https://doc.rust-lang.org/rust-by-example/std_misc/channels.html), this library adds seven specialized channel types that aren't provided by Go's built-in `chan` type:

| Package     | Senders | Receivers | Semantics                                                  |
| ----------- | ------- | --------- | ---------------------------------------------------------- |
| `oneshot`   | 1       | 1         | Single value, send-once. Cancellable from either side.     |
| `spsc`      | 1       | 1         | Single-producer / single-consumer queue.                   |
| `spmc`      | 1       | many      | Work distribution: each item goes to *one* receiver.       |
| `mpsc`      | many    | 1         | Fan-in: multiple-producer / single-consumer.               |
| `mpmc`      | many    | many      | General load-balanced queue.                               |
| `broadcast` | 1       | many      | Fan-out: every item delivered to *every* active receiver.  |
| `watch`     | 1       | many      | Latest-value-only, new sends overwrite unread ones.        |

## Installation

```console
go get github.com/amorey/gochan
```

Each architecture lives in its own subpackage:

```go
import (
    "github.com/amorey/gochan/mpsc"
    "github.com/amorey/gochan/broadcast"
)
```

Requires Go 1.21+.

## Basic Usage

**Oneshot** — return a single value from a goroutine:

```go
tx, rx, closeAll := oneshot.New[Result]()
defer closeAll()

go func() { tx.Send(compute()) }()
result, _ := rx.Recv()
```

**SPSC** — stream values between a single producer and a single consumer:

```go
tx, rx, closeAll := spsc.New[int](64)
defer closeAll()

go func() {
    for i := 0; i < 10; i++ { tx.Send(i) }
    tx.Close()
}()
for {
    v, err := rx.Recv()
    if err != nil { break }
    process(v)
}
```

**SPMC** — distribute work from one producer across a pool of workers:

```go
hub := spmc.New[Job](128)
defer hub.Close()
tx := hub.Sender()

for i := 0; i < workers; i++ {
    rx := hub.Receiver()
    go func() {
        defer rx.Close()
        for {
            job, err := rx.Recv()
            if err != nil { return }
            run(job)
        }
    }()
}
for _, j := range jobs { tx.Send(j) }
tx.Close()
```

**MPSC** — fan-in workers to a single collector:

```go
hub := mpsc.New[Event](256)
defer hub.Close()
rx := hub.Receiver()

for i := 0; i < n; i++ {
    s := hub.Sender()
    go func() { defer s.Close(); produce(s) }()
}
for {
    ev, err := rx.Recv()
    if err != nil { break }
    handle(ev)
}
```

**MPMC** — shared queue feeding a worker pool from many producers:

```go
hub := mpmc.New[Task](256)
defer hub.Close()

for i := 0; i < producers; i++ {
    s := hub.Sender()
    go func() { defer s.Close(); produce(s) }()
}
for i := 0; i < consumers; i++ {
    go func() {
        rx := hub.Receiver()
        for {
            t, err := rx.Recv()
            if err != nil { return }
            handle(t)
        }
    }()
}
```

**Broadcast** — fan an event out to every listener:

```go
hub := broadcast.New[Event](64)
defer hub.Close()

rx1 := hub.Receiver(); defer rx1.Close()
rx2 := hub.Receiver(); defer rx2.Close()

tx := hub.Sender()
tx.Send(Event{...}) // both receivers will see it
```

**Watch** — propagate the latest config:

```go
hub := watch.New[Config](initial)
defer hub.Close()
tx := hub.Sender()

go func() {
    for c := range updates { tx.Send(c) }
}()
rx := hub.Receiver()
defer rx.Close()
for {
    cfg, err := rx.Recv() // first call returns initial immediately
    if err != nil { break }
    apply(cfg)
}
```

## Design notes

Here are some design decisions to be aware of:

**Bounded by default**: Unbounded queues are a memory-safety footgun. Every queue-style constructor takes an explicit capacity.

**Queue-style channel capacity**: (`spsc`, `spmc`, `mpsc`, `mpmc`), capacity behaves exactly like Go's buffered channels because the implementation *is* a buffered Go channel underneath. `New[T](0)` is a rendezvous channel — `Send` blocks until a `Recv` is ready. `New[T](n)` allows `n` queued values before `Send` blocks.

**One close-all per package**: Every constructor exposes a single "close everything" entry point alongside the per-handle `Sender.Close()` and `Receiver.Close()`. Singleton-pair packages (`oneshot`, `spsc`) return a `func()` as the third value of their constructor; multi-side packages return a `*Hub[T]` whose `Close()` method serves the same purpose. In both cases the function is equivalent to calling `Close()` on every handle: the sender's close drains buffered values (`Chan` consumers drain then exit), while the receiver-side close immediately fails `Recv`/`TryRecv`/`RecvContext` with `ErrClosed`. Inherits the sender's close discipline — don't call it concurrently with an active `Send` from a different goroutine.

**Receivers come and go independently on fan-out hubs**: For `spmc`, `broadcast`, `watch`, and `mpmc`, each `hub.Receiver()` call returns a fresh subscriber. Calling `Close()` on a single receiver removes only that receiver from the consumer set; the sender and other receivers are unaffected. This lets listeners come and go independently of the sender's lifecycle.

**Broadcast uses a ring buffer**: Slow receivers don't block the sender — they get `ErrLagged` and skip forward.


## API

### Constructors

Singleton-pair packages (`oneshot`, `spsc`) return `(*Sender, *Receiver, func())` directly — both handles plus a close function that calls `Close` on each. Multi-side packages return a `*Hub[T]` that hands out `Sender` and `Receiver` handles via `hub.Sender()` / `hub.Receiver()` and exposes `hub.Close()` which closes every live handle.

| Constructor             | Returns                              | Capacity arg | `0` allowed?     |
| ----------------------- | ------------------------------------ | ------------ | ---------------- |
| `oneshot.New[T]()`      | `(*Sender[T], *Receiver[T], func())` | —            | —                |
| `spsc.New[T](n)`        | `(*Sender[T], *Receiver[T], func())` | yes          | yes (rendezvous) |
| `spmc.New[T](n)`        | `*Hub[T]`                            | yes          | yes (rendezvous) |
| `mpsc.New[T](n)`        | `*Hub[T]`                            | yes          | yes (rendezvous) |
| `mpmc.New[T](n)`        | `*Hub[T]`                            | yes          | yes (rendezvous) |
| `broadcast.New[T](n)`   | `*Hub[T]`                            | yes          | no (panics)      |
| `watch.New[T](initial)` | `*Hub[T]`                            | —            | —                |

### Common interfaces

Every `Sender` and `Receiver` implements the common interfaces defined in [`gochan`](./gochan.go), so call sites can be swapped between architectures with minimal churn:

```go
type Sender[T any] interface {
    Send(v T) error                              // blocks until delivered or closed
    TrySend(v T) error                           // returns ErrFull / ErrClosed immediately
    SendContext(ctx context.Context, v T) error  // blocks with cancellation
    Close()                                      // idempotent
}

type Receiver[T any] interface {
    Recv() (T, error)                            // blocks until received or closed
    TryRecv() (T, error)                         // returns ErrEmpty / ErrClosed immediately
    RecvContext(ctx context.Context) (T, error)  // blocks with cancellation
    Chan() <-chan T                              // native channel for use with select
    Close()                                      // idempotent
}
```

Multi-side packages (`spmc`, `mpsc`, `mpmc`, `broadcast`, `watch`) additionally implement `gochan.Hub[T]`:

```go
type Hub[T any] interface {
    Sender()   Sender[T]                         // returns a fresh handle (multi-side) or the singleton (single-side)
    Receiver() Receiver[T]                       // same
    Close()                                      // closes every live handle; idempotent
}
```

On singleton-side architectures (e.g. spmc's `Sender`, mpsc's `Receiver`), repeated calls return the same handle — `Sender()` and `Receiver()` are idempotent getters there. On multi-side architectures they hand out a fresh handle each call. After the hub has been closed, the returned handle reports `ErrClosed` on use; you don't have to check up-front.

Singleton-pair packages (`oneshot`, `spsc`) do not implement `gochan.Hub`. Their constructors return `(*Sender[T], *Receiver[T], func())` directly — both handles plus a close-all function — giving a compile-time guarantee that the handles cannot be requested twice.

The `Chan()` method on every receiver gives you a native `chan` for use in `select`. Closing a receiver does *not* close `Chan()` — use `Recv`/`TryRecv` if you need to observe receiver-close. Closing the sender (or calling the close-all) does close `Chan()` after the buffer drains, the same as `close()` on a raw Go channel.

### Close semantics

There are two close operations: per-handle (`Sender.Close()` / `Receiver.Close()`), and close-all (`Hub.Close()` or the function returned by `oneshot.New` / `spsc.New`). Close-all is exactly equivalent to calling `Close()` on every handle the hub has handed out — no separate semantics.

| Call                | Effect                                                                                                    |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| `Sender.Close()`    | Graceful end-of-stream. On queue-style channels, buffered values remain receivable; `Recv` and `Chan` drain them, then see `ErrClosed` / channel-closed. |
| `Receiver.Close()`  | This handle only. On multi-receiver hubs, other receivers and the sender keep running. Buffered values are abandoned for `Recv` on this handle. |
| close-all           | `Hub.Close()` on hub-style packages, or the third return value from the constructor on singleton-pair packages. Equivalent to receiver(s) `Close` then sender `Close`. For Recv-style callers, buffered values are abandoned (the receiver close runs first); for `Chan` consumers, the underlying channel is closed and they drain remaining values before seeing closed. |

All three are idempotent. The close-all inherits the sender's close discipline — don't call it concurrently with an active `Send` from a different goroutine.

### Errors

A small set of sentinel errors is shared across all packages:

```go
var ErrClosed   = errors.New("chans: channel closed")
var ErrFull     = errors.New("chans: channel full")
var ErrEmpty    = errors.New("chans: channel empty")
var ErrNotReady = errors.New("chans: no counterparty registered")

type ErrLagged struct{ Missed uint64 }  // broadcast only
```

`ErrNotReady` is returned by `TrySend` on packages with multi-side fan-out (`spmc`, `mpmc`) before any receiver has been registered, and by `TryRecv` on multi-side fan-in (`mpsc`, `mpmc`) before any sender has been registered. It distinguishes "the other side hasn't shown up yet" (a lifecycle/config condition) from `ErrFull` / `ErrEmpty` (transient buffer pressure). After the first registration on each side the regular `ErrFull` / `ErrEmpty` semantics apply.

`ErrLagged` is specific to `broadcast` and signals that a slow receiver has fallen behind the ring buffer; the receiver is still usable and will resume from the oldest still-buffered value.

### Per-package reference

Per-package API documentation — constructors, methods, semantics, and (for `broadcast`) common publishing patterns — lives with the code itself. Browse it on [pkg.go.dev/github.com/amorey/gochan](https://pkg.go.dev/github.com/amorey/gochan) or run `go doc github.com/amorey/gochan/<pkg>` locally.

