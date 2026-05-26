# gochan

*gochan is a small library for implementing multiple channel architectures for Go, inspired by Rust*

## Introduction

Go channels are extremely useful but they only ship with one type - mpmc (multiple-producer/multiple-consumer), buffered or un-buffered. This means that we often have to add higher level logic to our data structures in order to implement common patterns like single-shot, broadcast and watch. Inspired by [`Rust channels`](https://doc.rust-lang.org/rust-by-example/std_misc/channels.html), this library adds seven specialized channel types to Go that you can use to implement common architectures not provided by Go's built-in `chan` type:

| Package     | Senders | Receivers | Semantics                                                  |
| ----------- | ------- | --------- | ---------------------------------------------------------- |
| `oneshot`   | 1       | 1         | Single value, send-once. Cancellable from either side.     |
| `spsc`      | 1       | 1         | Single-producer / single-consumer queue.                   |
| `spmc`      | 1       | many      | Work distribution: each item goes to *one* receiver.       |
| `mpsc`      | many    | 1         | Fan-in: multiple-producer / single-consumer.               |
| `mpmc`      | many    | many      | General load-balanced queue.                               |
| `broadcast` | many    | many      | Fan-out: every item delivered to *every* active receiver.  |
| `watch`     | many    | many      | Latest-value-only, new sends overwrite unread ones.        |

## Installation

```
go get github.com/yourname/gochan
```

Each architecture lives in its own subpackage; import the ones you need:

```go
import (
    "github.com/yourname/gochan/mpsc"
    "github.com/yourname/gochan/broadcast"
)
```

Requires Go 1.21+.

## Basic Usage

**Oneshot** — return a value from a goroutine:

```go
tx, rx := oneshot.New[Result]()
go func() { tx.Send(compute()) }()
result, _ := rx.Recv()
```

**SPSC** — stream values between a single producer and consumer:

```go
tx, rx := spsc.NewBounded[int](64)
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

**SPMC** — distribute work from one producer across a pool of workers:

```go
tx, rx := spmc.NewBounded[Job](128)
for i := 0; i < workers; i++ {
    go func() {
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
tx, rx := mpsc.NewBounded[Event](256)
for i := 0; i < n; i++ {
    s := tx.Clone()
    go func() { defer s.Close(); produce(s) }()
}
tx.Close() // drop the original handle
for {
    ev, err := rx.Recv()
    if err != nil { break }
    handle(ev)
}
```

**MPMC** — shared queue feeding a worker pool from many producers:

```go
tx, rx := mpmc.NewBounded[Task](256)
for i := 0; i < producers; i++ {
    s := tx.Clone()
    go func() { defer s.Close(); produce(s) }()
}
tx.Close()
for i := 0; i < consumers; i++ {
    go func() {
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
tx := broadcast.New[Event](64)
rx1 := tx.Subscribe()
defer rx1.Close()
rx2 := tx.Subscribe()
defer rx2.Close()
tx.Send(Event{...}) // both receivers will see it
```

**Watch** — propagate the latest config:

```go
tx := watch.New[Config](initial)
go func() {
    for c := range updates { tx.Send(c) }
}()
for {
    rx := tx.Subscribe()
    defer rx.Close()
    cfg, err := rx.Recv() // first call returns initial immediately
    if err != nil { break }
    apply(cfg)
}
```

## Common interface

Every channel type implements the same two interfaces, so swapping architectures is easy:

```go
type Sender[T any] interface {
    Send(v T) error                              // blocks until delivered or closed
    TrySend(v T) error                           // returns ErrFull / ErrClosed immediately
    SendContext(ctx context.Context, v T) error  // blocks with cancellation
    Close()                                      // idempotent
}

type Receiver[T any] interface {
    Recv() (T, error)                            // blocks until received or closed
    TryRecv() (T, error)                         // returns ErrClosed immediately
    RecvContext(ctx context.Context) (T, error)  // blocks with cancellation
    Chan() <-chan T                              // native channel for use with select
    Close()                                      // idempotent
}
```

The `Chan()` method on every receiver let's you use a native `chan` instance to receive messages to enable easy integration into common Go workflows (e.g. `select`).

## Errors

A small set of sentinel errors is shared across all packages:

```go
var ErrClosed = errors.New("chans: channel closed")
var ErrFull   = errors.New("chans: channel full")
var ErrEmpty  = errors.New("chans: channel empty")

type ErrLagged struct{ Skipped uint64 }  // broadcast only
```

`ErrLagged` is specific to `broadcast` and signals that a slow receiver has fallen behind the ring buffer. The receiver is still usable and will resume from the oldest still-buffered value.

## Constructors

Each package exposes one or two constructors, some with explicit `capacity` arguments:

| Constructor              | Capacity arg | `0` allowed?     | Semantics                                    |
| ------------------------ | ------------ | ---------------- | -------------------------------------------- |
| `oneshot.New[T]()`       | —            | —                | Single value, single delivery                |
| `spsc.NewBounded[T](n)`  | yes          | yes (rendezvous) | FIFO queue, one sender and one receiver      |
| `spmc.NewBounded[T](n)`  | yes          | yes (rendezvous) | One sender, items load-balanced to receivers |
| `mpsc.NewBounded[T](n)`  | yes          | yes (rendezvous) | Fan-in from many senders to one receiver     |
| `mpsc.NewUnbounded[T]()` | —            | —                | Grows without bound                          |
| `mpmc.NewBounded[T](n)`  | yes          | yes (rendezvous) | Shared queue, any sender to any receiver     |
| `broadcast.New[T](n)`    | yes          | no (panics)      | Ring buffer size; overwrites on lag          |
| `watch.New[T](initial)`  | —            | —                | Single slot, always holds the latest value   |

`broadcast` and `watch` return only a sender; receivers are obtained via `tx.Subscribe()`. This matches their fan-out nature — receivers come and go independently of the sender's lifecycle.

## Design notes

Here are some design decisions to be aware of:

**Bounded by default**: Unbounded queues are a memory-safety footgun. Only `mpsc` offers an unbounded variant, and the doc comment warns about it. Everything else takes an explicit capacity.

**Queue-style channel capacity**: (`spsc`, `spmc`, `mpsc (bounded)`, `mpmc`), capacity behaves exactly like Go's buffered channels because the implementation *is* a buffered Go channel underneath. `NewBounded[T](0)` is a rendezvous channel — `Send` blocks until a `Recv` is ready. `NewBounded[T](n)` allows `n` queued values before `Send` blocks.

**`mpsc.NewUnbounded` grows as needed**: Use this when bursts are unavoidable and bounded back-pressure would deadlock you — but watch for memory growth if producers can outrun the consumer indefinitely.

**Broadcast and watch use Subscribe() semantics**: The `Sender` returned by `broadcast` and `watch` constructors exposes a `Subscribe()` method which returns a `Receiver`. Calling `Close()` on a subscriber receiver removes only that receiver from the subscriber set; the sender and other subscribers are unaffected. This lets listeners come and go independently of the sender's lifecycle.

**Broadcast uses a ring buffer**: Slow receivers don't block the sender — they get `ErrLagged` and skip forward.

## API

### oneshot

A single-value, single-delivery channel. Exactly one `Send` will ever succeed, and the value is delivered to exactly one `Recv`. Either side may cancel by closing its handle, and the other side is notified via `ErrClosed`.

Typical uses: returning a single result from a goroutine, request/response RPC-style handoff, "done" signalling with an attached value.

**Constructor**

```go
func New[T any]() (*Sender[T], *Receiver[T])
```

<dl>
  <dt><code>New[T]()</code></dt>
  <dd>Creates a fresh oneshot pair. The pair shares a single value slot. There is no capacity argument — a oneshot is always rendezvous-like, but <code>Send</code> does not block waiting for <code>Recv</code> (see below).</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Deposits <code>v</code> into the slot and returns immediately — it does <em>not</em> wait for a receiver. Returns <code>ErrClosed</code> if the receiver has already been closed, or if <code>Send</code>/<code>Close</code> has already been called on this sender. The value is consumed on success; on <code>ErrClosed</code> it is dropped.</dd>
  <dt><code>Close()</code></dt>
  <dd>Cancels the channel from the sender side. A pending or future <code>Recv</code> returns <code>ErrClosed</code>. Idempotent; safe to call after a successful <code>Send</code> (no-op).</dd>
</dl>

`TrySend` and `SendContext` call `Send` internally.

**Receiver**

```go
func (rx *Receiver[T]) Recv() (T, error)
func (rx *Receiver[T]) TryRecv() (T, error)
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error)
func (rx *Receiver[T]) Chan() <-chan T
func (rx *Receiver[T]) Close()
```

<dl>
  <dt><code>Recv() (T, error)</code></dt>
  <dd>Blocks until the value is sent. Returns <code>ErrClosed</code> if the sender closes without sending, or if <code>Recv</code> has already consumed the value on this receiver.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the value if already sent, <code>ErrEmpty</code> if not yet sent, or <code>ErrClosed</code> if the sender closed without sending or the value has already been consumed.</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if the context is cancelled first. Cancelling the context does <em>not</em> close the receiver; you can call <code>Recv</code> again afterwards.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns a native channel that will yield the value once and then be closed. Useful in <code>select</code>. Calling <code>Chan</code> multiple times returns the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Cancels the channel from the receiver side. A pending or future <code>Send</code> returns <code>ErrClosed</code> and the value (if any) is dropped. Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Single delivery</dt>
  <dd>After one successful <code>Send</code>/<code>Recv</code> pair, both sides are spent. Subsequent <code>Send</code> returns <code>ErrClosed</code>; subsequent <code>Recv</code> returns <code>ErrClosed</code>.</dd>
  <dt>Cancellation</dt>
  <dd>Either side may <code>Close</code> to abandon the exchange. The other side observes <code>ErrClosed</code> on its next operation. If <code>Send</code> and receiver-<code>Close</code> race, exactly one wins: either the value is delivered, or the value is dropped and <code>Send</code> returns <code>ErrClosed</code>.</dd>
  <dt>No goroutine leak</dt>
  <dd>Because <code>Send</code> does not block on a receiver, a sender that completes its work and then has its receiver vanish never leaks. Conversely, a <code>Recv</code> caller that wants to bail must use <code>RecvContext</code> or <code>Close</code>.</dd>
</dl>

### spsc

### spmc

### mpsc

### mpmc

### broadcast

### watch
