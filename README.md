# gochan

*Gochan is a small library of common channel architectures for Go, inspired by Rust*

<img width="435" alt="gochan" src="https://github.com/user-attachments/assets/55534faa-494e-4038-a093-6c37f7040693" />

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/gochan.svg)](https://pkg.go.dev/github.com/amorey/gochan)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Introduction

Go channels are extremely useful but they only ship with one type - mpmc (multiple-producer/multiple-consumer), buffered or un-buffered. This means that we often have to add higher level logic to our data structures in order to implement common patterns like oneshot, broadcasts and watches. Inspired by [`Rust channels`](https://doc.rust-lang.org/rust-by-example/std_misc/channels.html), this library adds seven specialized channel types that aren't provided by Go's built-in `chan` type:

| Package     | Senders | Receivers | Semantics                                                  |
| ----------- | ------- | --------- | ---------------------------------------------------------- |
| `oneshot`   | 1       | 1         | Single value, send-once. Cancellable from either side.     |
| `spsc`      | 1       | 1         | Single-producer / single-consumer queue.                   |
| `spmc`      | 1       | many      | Work distribution: each item goes to *one* receiver.       |
| `mpsc`      | many    | 1         | Fan-in: multiple-producer / single-consumer.               |
| `mpmc`      | many    | many      | General load-balanced queue.                               |
| `broadcast` | 1       | many      | Fan-out: every item delivered to *every* active receiver.  |
| `watch`     | 1       | many      | Latest-value-only, new sends overwrite unread ones.        |

With these types you can add common coordination patterns to your Go structs without writing custom code yourself.

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

## Channel Types

### Oneshot

Oneshot is a single-value, single-delivery non-blocking channel that delivers exactly one `Send()` to exactly one `Recv()`. Either side can cancel by closing its handle, and the other side will observe `ErrClosed` on its next operation. `Send()` does not block on a receiver — once the value is accepted into the slot the sender is free to move on, so a sender whose receiver vanishes never leaks. Typical uses: returning a single result from a goroutine, request/response handoff, or "done" signalling with an attached value.

```go
tx, rx := oneshot.New[Result]()
defer tx.Close()
defer rx.Close()

go func() { tx.Send(compute()) }()
result, err := rx.Recv()
if err != nil {
    // sender closed without sending
}
```

[Recv Example](./oneshot/examples/recv/main.go) · [Chan Example](./oneshot/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/oneshot)


### SPSC (Single-Producer/Single-Consumer)

SPSC is a bounded FIFO queue between exactly one sender and exactly one receiver. Capacity behaves like a Go buffered channel: `New[T](0)` is a rendezvous handoff and `New[T](n)` buffers up to `n` values before `Send()` blocks, applying natural backpressure. Typical uses: streaming pipelines between two cooperating goroutines, producer/consumer stages in a larger dataflow, decoupling a fast producer from a slow consumer with a fixed-size buffer.

```go
tx, rx := spsc.New[int](64)
defer rx.Close()

go func() {
    defer tx.Close()
    for i := 0; i < 10; i++ { tx.Send(i) }
}()
for {
    v, err := rx.Recv()
    if err != nil { break }
    process(v)
}
```

[Recv Example](./spsc/examples/recv/main.go) · [Chan Example](./spsc/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/spsc)

### SPMC (Single-Producer/Multiple-Consumer)

SPMC is a bounded FIFO queue with one sender feeding any number of receivers, where each value goes to *exactly one* receiver (i.e. load distribution, not fan-out). The hub hands out the singleton `Sender` via `hub.Sender()` and a fresh `Receiver` per worker via `hub.Receiver()`. Closing the sender lets receivers drain buffered values before observing `ErrClosed`. Closing a single receiver only removes that worker but if every receiver closes, the sender's next `Send()` returns `ErrClosed`. Typical uses: distributing work items from a single dispatcher to a worker pool, parallelizing a CPU-bound pipeline stage, fanning one input stream out across N consumers without duplication.

```go
hub := spmc.New[Job](128)
defer hub.Close()

tx := hub.Sender()
defer tx.Close()

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
```

[Recv Example](./spmc/examples/recv/main.go) · [Chan Example](./spmc/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/spmc)

### MPSC (Multiple-Producer/Single-Consumer)

MPSC is a bounded FIFO queue with any number of senders feeding a single receiver, where every value lands at the same consumer. The hub mints a fresh `Sender` per producer via `hub.Sender()` and exposes the singleton `Receiver` via `hub.Receiver()`. The queue preserves arrival order at the underlying channel, but the relative order of sends from *different* producers is scheduling-dependent — only sends from a single producer are ordered with respect to each other. Closing a single sender only removes that producer and the receiver drains buffered values and observes `ErrClosed` once every registered sender has closed. Closing the receiver immediately fails all pending and future `Send()` calls with `ErrClosed`. Typical uses: fan-in of events from many worker goroutines into one aggregator, collecting results from a scatter of parallel tasks, funnelling log/metric streams to a single writer.

```go
hub := mpsc.New[Event](256)
defer hub.Close()

rx := hub.Receiver()
defer rx.Close()

for i := 0; i < n; i++ {
    tx := hub.Sender()
    go func() { defer tx.Close(); produce(tx) }()
}
for {
    ev, err := rx.Recv()
    if err != nil { break }
    handle(ev)
}
```

[Recv Example](./mpsc/examples/recv/main.go) · [Chan Example](./mpsc/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/mpsc)

### MPMC (Multiple-Producer/Multiple-Consumer)

MPMC is a bounded FIFO queue with any number of senders and any number of receivers, where each value is delivered to *exactly one* receiver (i.e. load distribution, not fan-out). The hub mints a fresh `Sender` per producer via `hub.Sender()` and a fresh `Receiver` per worker via `hub.Receiver()`. Closing a single sender or receiver only removes that handle. Teceivers observe `ErrClosed` once every registered sender has closed and the buffer is drained, and senders observe `ErrClosed` once every registered receiver has closed. Typical uses: work queues with elastic producer and consumer pools, ingestion pipelines where N publishers feed M workers, generic job/task queues without a designated dispatcher.

```go
hub := mpmc.New[Task](256)
defer hub.Close()

for i := 0; i < producers; i++ {
    tx := hub.Sender()
    go func() { defer tx.Close(); produce(tx) }()
}
for i := 0; i < workers; i++ {
    rx := hub.Receiver()
    go func() {
        defer rx.Close()
        for {
            t, err := rx.Recv()
            if err != nil { return }
            run(t)
        }
    }()
}
```

[Recv Example](./mpmc/examples/recv/main.go) · [Chan Example](./mpmc/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/mpmc)

### Broadcast

Broadcast is a fan-out channel backed by a fixed-size ring buffer: every value published through the singleton `Sender` is delivered to *every* live `Receiver`. The hub hands out the singleton sender via `hub.Sender()` (safe to share across publisher goroutines) and a fresh `Receiver` per subscriber via `hub.Receiver()`. `Send()` never blocks — when the ring wraps onto an unread slot the oldest unread value is overwritten and the affected receiver observes `ErrLagged` on its next `Recv()` before resuming from the oldest still-buffered value. `TrySend()` exposes the same condition as `ErrFull` so publishers can self-throttle or implement drop-newest. Late subscribers start at the current write position and do not see historical values. `New[T](0)` panics. Typical uses: event-stream fan-out to many listeners, configuration-change notifications, market-data ticks to many strategies, WebSocket/SSE push systems where slow clients must not back up the publisher.

```go
hub := broadcast.New[Event](64)
defer hub.Close()

tx := hub.Sender()
defer tx.Close()

for i := 0; i < listeners; i++ {
    rx := hub.Receiver()
    go func() {
        defer rx.Close()
        for {
            ev, err := rx.Recv()
            if err == gochan.ErrLagged { continue }
            if err != nil { return }
            handle(ev)
        }
    }()
}
for _, e := range events { tx.Send(e) }
```

[Recv Example](./broadcast/examples/recv/main.go) · [Chan Example](./broadcast/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/broadcast)

### Watch

Watch is a single-producer, multi-consumer latest-value channel: the hub maintains a single slot that holds the current value, and each `Send()` overwrites it. Receivers see the slot's contents rather than a stream. This means that if the sender publishes A, B, C in rapid succession and a receiver only calls `Recv()` once afterwards, it sees C (intermediate values are silently dropped). The hub is seeded with an initial value at construction and every receiver's first `Recv()` returns the current value immediately without waiting for a send, so new subscribers bootstrap right away. `Send()` never blocks, so slow receivers cannot apply backpressure to the publisher. Closing the sender delivers the final value once to each receiver that hasn't yet observed it before subsequent calls return `ErrClosed`. Typical uses: configuration / settings propagation, "current state" distribution (current leader, connection status, feature flags), shutdown / cancellation signals carrying a final state.

```go
hub := watch.New[Config](initial)
defer hub.Close() // convenience for hub.Sender().Close()

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

[Recv Example](./watch/examples/recv/main.go) · [Chan Example](./watch/examples/chan/main.go) · [Docs](https://pkg.go.dev/github.com/amorey/gochan/watch)

## Design notes

### Common interfaces

Every `Sender` and `Receiver` implements the common interfaces in [`gochan`](./gochan.go), so call sites can be swapped between architectures more easily:

```go
type Sender[T any] interface {
    Send(v T) error                              // blocks until accepted or closed (oneshot/broadcast/watch never block)
    TrySend(v T) error                           // returns ErrFull / ErrClosed / ErrNotReady immediately
    SendContext(ctx context.Context, v T) error  // blocks with cancellation
    Close()                                      // idempotent
}

type Receiver[T any] interface {
    Recv() (T, error)                            // blocks until received or closed
    TryRecv() (T, error)                         // returns ErrEmpty / ErrClosed / ErrNotReady / ErrLagged immediately
    RecvContext(ctx context.Context) (T, error)  // blocks with cancellation
    Chan() <-chan T                              // native channel for use with select
    Close()                                      // idempotent
}
```

Multi-side packages (`spmc`, `mpsc`, `mpmc`, `broadcast`, `watch`) each expose their own concrete `*Hub[T]`. There is intentionally no shared `Hub` interface — semantics differ enough (e.g. `mpmc` drops nothing, `broadcast` returns `ErrLagged`) that swapping behind one interface would be a footgun. Every hub has the same shape:

```go
// On each package's *Hub[T]:
Sender()   *Sender[T]    // fresh handle on multi-Sender packages; the singleton on single-Sender packages
Receiver() *Receiver[T]  // same shape for the receive side
Close()                  // closes every live handle; idempotent
```

Singleton-side getters (e.g. spmc's `Sender`, mpsc's `Receiver`) return the same handle on repeated calls; multi-side getters mint fresh handles. After `Hub.Close()`, returned handles report `ErrClosed` on use.

Singleton-pair packages (`oneshot`, `spsc`) have no hub: constructors return `(*Sender[T], *Receiver[T])` directly, and each side is closed via its own `Close()`.

### Errors

```go
var ErrClosed   = errors.New("gochan: channel closed")
var ErrFull     = errors.New("gochan: channel full")
var ErrEmpty    = errors.New("gochan: channel empty")
var ErrNotReady = errors.New("gochan: no counterparty registered")

type ErrLagged struct{ Missed uint64 }  // broadcast only
```

`ErrNotReady` is returned by `TrySend` on fan-out packages (`spmc`, `mpmc`) before any receiver is registered, and by `TryRecv` on fan-in packages (`mpsc`, `mpmc`) before any sender is registered — distinguishing "no counterparty yet" from transient `ErrFull`/`ErrEmpty`. `ErrLagged` is `broadcast`-only: the slow receiver fell behind the ring buffer and resumes from the oldest still-buffered value.

### Close semantics

| Call                | Effect                                                                                                    |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| `Sender.Close()`    | Graceful end-of-stream. On queue-style channels, `Recv` and `Chan` drain buffered values, then see `ErrClosed` / channel-closed. |
| `Receiver.Close()`  | This handle only. Other receivers and the sender keep running; buffered values are abandoned for this handle. |
| `Hub.Close()`       | Hub-style packages. Equivalent to closing every receiver then the sender: `Recv` sees `ErrClosed` immediately; queue-style `Chan` drains, per-receiver-feeder `Chan` (`broadcast`, `watch`) closes without draining. For `watch`, use `Sender.Close()` instead if receivers should observe the latest value once before shutdown. |

All idempotent. Don't call `Hub.Close` concurrently with an active `Send` from another goroutine — it inherits the sender's close discipline.

### Thread safety

**`oneshot`, `spsc`, `spmc`, `mpsc`, `mpmc`**: Concurrent `Send()`/`Close()` (or `Recv()`/`Close()`) on the same handle is not supported. To avoid any cross-thread race conditions, don't share handles across goroutines.

**`broadcast`, `watch`**: Concurrent `Send()`/`Close()` is safe to share across goroutines.

### Chan support

`Chan()` comes in two flavors:

- **Queue-style** (`spsc`, `spmc`, `mpsc`, `mpmc`): exposes the underlying buffered channel. `Receiver.Close()` does *not* close it — use `Recv`/`TryRecv` to observe receiver-close. It closes only when the sender closes and the buffer drains.

- **Per-receiver feeder** (`broadcast`, `watch`): private channel fed by a per-receiver goroutine. `Receiver.Close()` *does* close it; always `Close` the receiver when done or the feeder leaks.

For `oneshot`, `Chan()` is the one-slot delivery channel; sender-close closes it after the value is observed.
