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
| `broadcast` | many    | many      | Fan-out: every item delivered to *every* active receiver.  |
| `watch`     | 1       | many      | Latest-value-only, new sends overwrite unread ones.        |

## Installation

```
go get github.com/amorey/gochan
```

Each architecture lives in its own subpackage; import the ones you need:

```go
import (
    "github.com/amorey/gochan/mpsc"
    "github.com/amorey/gochan/broadcast"
)
```

Requires Go 1.21+.

## Basic Usage

**Oneshot** — return a value from a goroutine:

```go
tx, rx, closeAll := oneshot.New[Result]()
defer closeAll()

go func() { tx.Send(compute()) }()
result, _ := rx.Recv()
```

**SPSC** — stream values between a single producer and consumer:

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

### Packages

#### oneshot

A single-value, single-delivery channel. Exactly one `Send` will ever succeed, and the value is delivered to exactly one `Recv`. Either side may cancel by closing its handle, and the other side is notified via `ErrClosed`.

Typical uses: returning a single result from a goroutine, request/response RPC-style handoff, "done" signalling with an attached value.

**Constructor**

```go
func New[T any]() (*Sender[T], *Receiver[T], func())
```

<dl>
  <dt><code>New[T]()</code></dt>
  <dd>Creates a fresh oneshot pair: a sender, a receiver, and a close function that calls <code>Close</code> on both. There is no capacity argument — a oneshot is always rendezvous-like, but <code>Send</code> does not block waiting for <code>Recv</code> (see below). The close function is idempotent and safe to defer.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Deposits <code>v</code> into the slot and returns immediately — it does <em>not</em> wait for a receiver. Returns <code>ErrClosed</code> if the receiver has already been closed, or if <code>Send</code>/<code>Close</code> has already been called on this sender. The value is consumed on success; on <code>ErrClosed</code> it is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Equivalent to <code>Send</code> for oneshot: <code>Send</code> never blocks, so there is no separate non-blocking path. Provided to satisfy the common <code>Sender</code> interface.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Returns <code>ctx.Err()</code> if <code>ctx</code> is already cancelled and the value is not deposited; otherwise behaves like <code>Send</code>. Because <code>Send</code> never blocks, there is nothing for cancellation to interrupt mid-call — the context is only checked at entry.</dd>
  <dt><code>Close()</code></dt>
  <dd>Cancels the channel from the sender side. A pending or future <code>Recv</code> returns <code>ErrClosed</code>. Idempotent; safe to call after a successful <code>Send</code> (no-op).</dd>
</dl>

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
  <dd>Returns a native channel that yields the value once and then closes, or closes empty if the channel is cancelled before a successful <code>Send</code>. Useful in <code>select</code>. Calling <code>Chan</code> multiple times returns the same channel. If <code>Chan</code> is used, the value is delivered there and a subsequent <code>Recv</code> on the same receiver will return <code>ErrClosed</code> — pick one consumption mechanism per receiver.</dd>
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
  <dt>Close-all</dt>
  <dd>The close function returned by <code>New</code> calls <code>Close</code> on both handles. Pending value is dropped, both <code>Send</code> and <code>Recv</code> return <code>ErrClosed</code>. Convenient as <code>defer</code>.</dd>
</dl>

#### spsc

A single-producer, single-consumer FIFO queue. One `Sender` feeds values in
order to one `Receiver`. Capacity behaves exactly like a Go buffered channel:
`New[T](0)` is a rendezvous channel, `New[T](n)` allows `n`
queued values before `Send` blocks.

Typical uses: streaming pipelines between two cooperating goroutines, a
producer/consumer stage in a larger dataflow, decoupling a fast producer from
a slow consumer with a fixed-size buffer.

**Constructor**

```go
func New[T any](capacity int) (*Sender[T], *Receiver[T], func())
```

<dl>
  <dt><code>New[T](capacity)</code></dt>
  <dd>Creates a fresh spsc pair backed by a buffered Go channel of the given <code>capacity</code>, returning a sender, a receiver, and a close function that calls <code>Close</code> on both (Receiver first, then Sender, so an in-flight <code>Send</code> escapes via the receiver-close signal before the underlying channel is closed). <code>capacity == 0</code> yields a rendezvous channel where <code>Send</code> blocks until a matching <code>Recv</code> is ready. <code>capacity &lt; 0</code> panics. The close function is idempotent and safe to defer; it inherits the sender's close discipline — don't call it concurrently with an active <code>Send</code> from a different goroutine.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Enqueues <code>v</code>, blocking while the buffer is full. Returns <code>ErrClosed</code> if the sender or receiver has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrFull</code> if the buffer is full, <code>ErrClosed</code> if closed, or <code>nil</code> on success.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the sender. Already-queued values remain receivable; a subsequent <code>Recv</code> drains them and then returns <code>ErrClosed</code>. Further <code>Send</code> calls return <code>ErrClosed</code>. Idempotent. Intended to be called by the single producer — spsc does not synchronize concurrent callers on the sender side.</dd>
</dl>

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
  <dd>Blocks until a value is available. Returns the next value in FIFO order, or <code>ErrClosed</code> if the buffer is empty and the sender has closed, or if the receiver has been closed.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrEmpty</code> if the buffer is empty but the sender is still open, or <code>ErrClosed</code> if the buffer is empty and closed.</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close the receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. The channel is closed when the sender closes (directly or via the close function returned by <code>New</code>) and the buffer drains. Closing the receiver does <em>not</em> close this channel — use <code>Recv</code>/<code>TryRecv</code> if you need to observe receiver-close. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the receiver. Pending or future <code>Send</code> calls return <code>ErrClosed</code>, and subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls return <code>ErrClosed</code>. Any values still buffered are abandoned. Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Single producer / single consumer</dt>
  <dd>Exactly one goroutine should call <code>Send</code>/<code>Close</code> on the sender and exactly one goroutine should call <code>Recv</code>/<code>Close</code> on the receiver. The implementation does not synchronize multiple concurrent callers on the same side.</dd>
  <dt>FIFO ordering</dt>
  <dd>Values are received in the exact order they were sent.</dd>
  <dt>Drain on sender close</dt>
  <dd>Closing the sender does not discard already-buffered values; the receiver drains them in order via <code>Recv</code> (or <code>Chan</code>) before observing <code>ErrClosed</code>. Closing the <em>receiver</em>, by contrast, abandons buffered values for <code>Recv</code>-style callers (though <code>Chan</code> consumers can still drain).</dd>
  <dt>Backpressure</dt>
  <dd>A bounded buffer applies natural backpressure: when full, <code>Send</code> blocks until the consumer makes room. Use <code>capacity == 0</code> for strict rendezvous handoff with no buffering.</dd>
  <dt>Close-all</dt>
  <dd>The close function returned by <code>New</code> calls <code>rx.Close()</code> then <code>tx.Close()</code>. Unblocks both sides with <code>ErrClosed</code> (Recv-style abandons buffered values; <code>Chan</code> consumers drain remaining values then see channel-closed).</dd>
</dl>

#### spmc

A single-producer, multi-consumer FIFO queue. One `Sender` feeds values into
a bounded buffer that is drained by any number of `Receiver`s, with each
value delivered to exactly one receiver. Capacity behaves exactly like a Go
buffered channel: `New[T](0)` is a rendezvous channel,
`New[T](n)` allows `n` queued values before `Send` blocks.

Typical uses: distributing work items to a pool of workers from a single
dispatcher goroutine, parallelizing a CPU-bound pipeline stage, fanning a
single input stream out across N consumers without duplication.

Unlike `broadcast`, every value goes to *one* receiver, not all of them —
`spmc` is load distribution, not fan-out.

**Constructor**

```go
func New[T any](capacity int) *Hub[T]
```

<dl>
  <dt><code>New[T](capacity)</code></dt>
  <dd>Creates a fresh spmc hub backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where <code>Send</code> blocks until some receiver is ready. <code>capacity &lt; 0</code> panics. A freshly constructed hub has no receivers, so <code>Send</code> will block (or <code>TrySend</code> will report <code>ErrFull</code>) until at least one <code>Receiver</code> is registered via <a href="#spmc-receiver"><code>hub.Receiver()</code></a>.</dd>
</dl>

**Hub**

```go
func (h *Hub[T]) Sender()   gochan.Sender[T]
func (h *Hub[T]) Receiver() gochan.Receiver[T]
func (h *Hub[T]) Close()
```

<dl>
  <dt><code>Sender() gochan.Sender[T]</code></dt>
  <dd>Returns the singleton send-side handle. Repeated calls return the same handle. The handle is safe to share across goroutines — <code>Send</code>, <code>TrySend</code>, <code>SendContext</code>, and <code>Close</code> may all be called concurrently from any number of publishers. After the hub has been closed (explicitly via <code>Hub.Close</code>, or implicitly because every previously-registered receiver has already closed) the returned handle reports <code>ErrClosed</code> on use.</dd>
  <dt id="spmc-receiver"><code>Receiver() gochan.Receiver[T]</code></dt>
  <dd>Returns a new receiver bound to the shared queue. Use this to add workers to the consumer pool. Each returned receiver has its own independent <code>Close</code> state but shares the queue and sender-close signal with every other receiver. Safe to call concurrently. If the hub has been closed (or every previously-registered receiver has already been closed) the returned handle is pre-closed and reports <code>ErrClosed</code> on use.</dd>
  <dt><code>Close()</code></dt>
  <dd>Equivalent to calling <code>Close</code> on every live receiver and on the sender (receivers first so an in-flight <code>Send</code> escapes via the dead signal before the underlying channel is closed). For <code>Recv</code>-style callers buffered values are abandoned; <code>Chan</code> consumers can drain them before seeing channel-closed. Future <code>Sender</code> calls return the (now-closed) singleton handle and future <code>Receiver</code> calls return pre-closed handles. Idempotent and safe to call concurrently with <code>Send</code> on any goroutine.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Enqueues <code>v</code>, blocking while the buffer is full and no receiver is ready to consume it. Returns <code>ErrClosed</code> if the sender has been closed, every receiver has been closed, or the hub has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrNotReady</code> if no receiver has yet been registered, <code>ErrFull</code> if the buffer is full and no receiver is currently parked on a recv, <code>ErrClosed</code> if closed, or <code>nil</code> on success.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the sender. Already-queued values remain receivable; receivers drain them in order before observing <code>ErrClosed</code>. Further <code>Send</code> calls return <code>ErrClosed</code>. Idempotent. Intended to be called by the single producer — spmc does not synchronize concurrent callers on the sender side (though <code>hub.Receiver()</code> is safe to call from any goroutine).</dd>
</dl>

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
  <dd>Blocks until a value is available to this receiver. Returns the next value in the shared FIFO, or <code>ErrClosed</code> if the buffer is empty and the sender has closed, this receiver is closed, or the hub has been closed. Each enqueued value is delivered to exactly one receiver; competing receivers are scheduled by the Go runtime.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrEmpty</code> if the buffer is empty but the sender is still open, or <code>ErrClosed</code> if the buffer is empty and closed (or this receiver/the hub is closed).</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close this receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. The channel is shared across all receivers in the same hub — values delivered on it count against the single shared queue, so two receivers selecting on <code>Chan()</code> simultaneously still see each value only once. Closed when the sender closes (directly or via <code>hub.Close()</code>) and the buffer drains. Closing <em>this</em> receiver does <em>not</em> close the channel; use <code>Recv</code>/<code>TryRecv</code> if you need that signal. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this receiver only. Other receivers and the sender are unaffected — they continue to consume and produce. Subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls on this handle return <code>ErrClosed</code>. The sender observes <code>ErrClosed</code> once <em>every</em> receiver has been closed (or the hub itself has been closed). Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Shared singleton sender, multi consumer</dt>
  <dd>The send-side handle returned by <code>hub.Sender()</code> is a singleton that is safe to share across goroutines: any number of publishers may call <code>Send</code> / <code>TrySend</code> / <code>SendContext</code> / <code>Close</code> concurrently on the same handle. Any number of goroutines may each hold their own <code>*Receiver[T]</code> (obtained from <code>hub.Receiver()</code>) and call <code>Recv</code>/<code>Close</code> on it; the implementation does not synchronize concurrent callers on the <em>same</em> receiver handle — call <code>hub.Receiver()</code> once per worker.</dd>
  <dt>Each value to exactly one receiver</dt>
  <dd>Values are <em>not</em> broadcast. A <code>Send</code> deposits one value into the shared queue, and the next <code>Recv</code> on any receiver removes it. Choice of receiver is non-deterministic and not guaranteed to be fair — it follows Go's channel-receive scheduling.</dd>
  <dt>FIFO ordering across the queue</dt>
  <dd>The queue itself preserves send order, but because work is split across multiple consumers, any single receiver only sees a subsequence of the sends — interleaved with the work other receivers grabbed.</dd>
  <dt>Sender close drains</dt>
  <dd>Closing the sender does not discard already-buffered values; receivers drain them in order before any of them observes <code>ErrClosed</code>. Closing a single receiver, by contrast, only affects that receiver; the queue keeps flowing through the remaining ones.</dd>
  <dt>All receivers closed ⇒ sender sees ErrClosed</dt>
  <dd>If every receiver has been closed, the sender's next <code>Send</code>/<code>TrySend</code>/<code>SendContext</code> returns <code>ErrClosed</code> and the value is dropped. This is how a sender notices that there is nobody left to receive its work.</dd>
  <dt>Backpressure</dt>
  <dd>A bounded buffer applies natural backpressure: when full, <code>Send</code> blocks until some receiver makes room. Use <code>capacity == 0</code> for strict rendezvous handoff with no buffering — a useful pattern when you want producers to slow down to exactly the rate of the slowest combined consumer throughput.</dd>
  <dt>Hub close-all</dt>
  <dd><code>Hub.Close()</code> calls <code>Close</code> on every live receiver and on the sender. Recv-style callers see <code>ErrClosed</code> immediately; <code>Chan</code> consumers drain remaining values before seeing channel-closed.</dd>
</dl>

#### mpsc

A multiple-producer, single-consumer FIFO queue. Any number of `Sender` handles feed values into a shared, fixed-capacity buffer drained by one `Receiver`. Capacity behaves exactly like a Go buffered channel: `New[T](0)` is a rendezvous channel, `New[T](n)` allows `n` queued values before `Send` blocks.

Typical uses: fan-in of events from many worker goroutines into a single
aggregator, collecting results from a scatter of parallel tasks, funnelling
log/metric/event streams to one writer.

**Constructor**

```go
func New[T any](capacity int) *Hub[T]
```

<dl>
  <dt><code>New[T](capacity)</code></dt>
  <dd>Creates a fresh mpsc hub backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where every <code>Send</code> blocks until the receiver is ready. <code>capacity &lt; 0</code> panics. Bounded mpsc gives you natural backpressure: when the buffer is full, producers wait.</dd>
</dl>

A freshly constructed mpsc hub has no senders, so <code>Recv</code> will block (or <code>TryRecv</code> will report <code>ErrEmpty</code>) until at least one producer is registered via <a href="#mpsc-sender"><code>hub.Sender()</code></a> and sends a value. The "all senders closed ⇒ <code>ErrClosed</code>" rule only kicks in once at least one sender has been registered — a fresh hub is not implicitly closed.

**Hub**

```go
func (h *Hub[T]) Sender()   gochan.Sender[T]
func (h *Hub[T]) Receiver() gochan.Receiver[T]
func (h *Hub[T]) Close()
```

<dl>
  <dt id="mpsc-sender"><code>Sender() gochan.Sender[T]</code></dt>
  <dd>Returns a new sender bound to the shared queue. Use this to add producers to the fan-in. Each returned sender has its own independent <code>Close</code> state but shares the queue and receiver-close signal with every other sender. Safe to call concurrently. If the hub has been closed (explicitly via <code>Hub.Close</code>, or implicitly because every previously-registered sender has already closed) or the receiver has been closed, the returned handle is pre-closed and reports <code>ErrClosed</code> on use.</dd>
  <dt><code>Receiver() gochan.Receiver[T]</code></dt>
  <dd>Returns the singleton receive-side handle. Repeated calls return the same handle. After the hub has been closed (explicitly via <code>Hub.Close</code>, or implicitly because every previously-registered sender has already closed) the returned handle reports <code>ErrClosed</code> on use.</dd>
  <dt><code>Close()</code></dt>
  <dd>Equivalent to calling <code>Close</code> on every live sender and on the receiver (receiver first, so an in-flight <code>Send</code> escapes via the dead signal before the underlying channel is closed). For <code>Recv</code>-style callers buffered values are abandoned; <code>Chan</code> consumers can drain them before seeing channel-closed. Future <code>Sender</code> calls return pre-closed handles and future <code>Receiver</code> calls return the (now-closed) singleton handle. Idempotent. Inherits the senders' close discipline — don't call concurrently with an active <code>Send</code> on any sender from another goroutine.</dd>
</dl>

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
  <dd>Blocks until a value is available. Returns the next value in FIFO order, or <code>ErrClosed</code> if the buffer is empty and every sender has closed, this receiver is closed, or the hub has been closed.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrNotReady</code> if no sender has yet been registered, <code>ErrEmpty</code> if the buffer is empty but at least one sender is still open, or <code>ErrClosed</code> if the buffer is empty and all senders are closed (or the receiver/hub is closed).</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close the receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. Closed when every sender has closed and the buffer has drained. Closing the receiver or hub does <em>not</em> close this channel — use <code>Recv</code>/<code>TryRecv</code> if you need those signals. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the receiver. Pending or future <code>Send</code> calls on any sender return <code>ErrClosed</code>, and subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls return <code>ErrClosed</code>. Any values still buffered are abandoned. Idempotent.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Enqueues <code>v</code>, blocking while the buffer is full. Returns <code>ErrClosed</code> if this sender has been closed, the receiver has been closed, or the hub has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrFull</code> if the buffer is full, <code>ErrClosed</code> if closed, or <code>nil</code> on success.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this sender only. Other senders continue to produce. Subsequent <code>Send</code>/<code>TrySend</code>/<code>SendContext</code> calls on this handle return <code>ErrClosed</code>. The receiver only observes <code>ErrClosed</code> (after draining) when <em>every</em> sender has been closed. Idempotent. Intended to be called by the producer goroutine that owns this sender — mpsc does not synchronize concurrent callers on the same sender handle.</dd>
</dl>

**Semantics**

<dl>
  <dt>Multi producer / single consumer</dt>
  <dd>Any number of goroutines may each hold their own <code>*Sender[T]</code> (obtained from <code>hub.Sender()</code>) and call <code>Send</code>/<code>Close</code> on it. Exactly one goroutine should call <code>Recv</code>/<code>Close</code> on the receiver. The implementation does not synchronize concurrent callers on the <em>same</em> sender handle — call <code>hub.Sender()</code> once per producer goroutine.</dd>
  <dt>FIFO across the queue, not across producers</dt>
  <dd>The queue itself preserves the order in which sends arrive at the underlying channel, but the relative ordering of sends from <em>different</em> producers is not defined — it depends on scheduling. Sends from a single producer remain in order with respect to each other.</dd>
  <dt>All senders closed ⇒ receiver drains, then sees ErrClosed</dt>
  <dd>Once at least one sender has been registered, the receiver observes <code>ErrClosed</code> when both (a) every sender obtained from <code>hub.Sender()</code> has been closed and (b) the buffer is empty. A freshly constructed hub with zero senders ever registered is <em>not</em> treated as closed — <code>Recv</code> blocks waiting for the first producer. If you spawn N producers, you must close all N — a forgotten <code>Close</code> on any one of them leaves the receiver waiting forever for an EOF that never arrives.</dd>
  <dt>Receiver close stops everything</dt>
  <dd>Closing the receiver immediately fails every pending and future <code>Send</code> across all senders with <code>ErrClosed</code> and abandons any buffered values.</dd>
  <dt>Backpressure</dt>
  <dd>A bounded buffer applies natural backpressure: when full, <code>Send</code> blocks until the consumer makes room. Use <code>capacity == 0</code> for strict rendezvous handoff with no buffering.</dd>
  <dt>Hub close-all</dt>
  <dd><code>Hub.Close()</code> calls <code>Close</code> on every live sender and on the receiver. Recv-style callers see <code>ErrClosed</code> immediately; <code>Chan</code> consumers drain remaining values before seeing channel-closed.</dd>
</dl>

#### mpmc

A multi-producer, multi-consumer FIFO queue. Any number of `Sender` handles
feed values into a bounded buffer drained by any number of `Receiver`
handles, with each value delivered to exactly one receiver. Capacity behaves
exactly like a Go buffered channel: `New[T](0)` is a rendezvous channel,
`New[T](n)` allows `n` queued values before `Send` blocks.

Typical uses: work queues with both elastic producer and consumer pools,
ingestion pipelines where N publishers feed M workers, generic job/task
queues without a designated dispatcher.

Like `spmc`, every value goes to *one* receiver, not all of them — `mpmc`
is load distribution across the consumer pool, not fan-out. If you need
fan-out, use `broadcast`.

**Constructor**

```go
func New[T any](capacity int) *Hub[T]
```

<dl>
  <dt><code>New[T](capacity)</code></dt>
  <dd>Creates a fresh mpmc hub backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where <code>Send</code> blocks until some receiver is ready. <code>capacity &lt; 0</code> panics.</dd>
</dl>

A freshly constructed mpmc hub has neither senders nor receivers. <code>Send</code> blocks (and <code>TrySend</code> reports <code>ErrFull</code>) until at least one <code>Receiver</code> is registered via <a href="#mpmc-receiver"><code>hub.Receiver()</code></a>; <code>Recv</code> blocks (and <code>TryRecv</code> reports <code>ErrEmpty</code>) until at least one <code>Sender</code> is registered via <a href="#mpmc-sender"><code>hub.Sender()</code></a> and sends a value. The "all senders closed ⇒ <code>ErrClosed</code>" and "all receivers closed ⇒ <code>ErrClosed</code>" rules only kick in after each side has had at least one registration — a fresh hub is not implicitly closed.

**Hub**

```go
func (h *Hub[T]) Sender()   gochan.Sender[T]
func (h *Hub[T]) Receiver() gochan.Receiver[T]
func (h *Hub[T]) Close()
```

<dl>
  <dt id="mpmc-sender"><code>Sender() gochan.Sender[T]</code></dt>
  <dd>Returns a new sender bound to the shared queue. Use this to add producers to the fan-in. Each returned sender has its own independent <code>Close</code> state but shares the queue and receiver-close signal with every other sender. Safe to call concurrently. If the hub has been closed (explicitly via <code>Hub.Close</code>, implicitly because every previously-registered sender has already closed, or because every previously-registered receiver has already closed) the returned handle is pre-closed and reports <code>ErrClosed</code> on use.</dd>
  <dt id="mpmc-receiver"><code>Receiver() gochan.Receiver[T]</code></dt>
  <dd>Returns a new receiver bound to the shared queue. Use this to add workers to the consumer pool. Each returned receiver has its own independent <code>Close</code> state but shares the queue and sender-close signal with every other receiver. Safe to call concurrently. If the hub has been closed (or every previously-registered receiver has already been closed, or every previously-registered sender has already closed and the buffer is empty) the returned handle is pre-closed and reports <code>ErrClosed</code> on use.</dd>
  <dt><code>Close()</code></dt>
  <dd>Equivalent to calling <code>Close</code> on every live receiver and on every live sender (receivers first, so in-flight <code>Send</code>s escape via the dead signal before the underlying channel is closed). For <code>Recv</code>-style callers buffered values are abandoned; <code>Chan</code> consumers can drain them before seeing channel-closed. Future <code>Sender</code> and <code>Receiver</code> calls return pre-closed handles. Idempotent. Inherits the senders' close discipline — don't call concurrently with an active <code>Send</code> on any sender from another goroutine.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Enqueues <code>v</code>, blocking while the buffer is full and (until the first receiver registers) while no receiver exists. Returns <code>ErrClosed</code> if this sender has been closed, every receiver has been closed, or the hub has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrNotReady</code> if no receiver has yet been registered, <code>ErrFull</code> if the buffer is full and no receiver is currently parked, <code>ErrClosed</code> if closed, or <code>nil</code> on success.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this sender only. Other senders continue to produce. Subsequent <code>Send</code>/<code>TrySend</code>/<code>SendContext</code> calls on this handle return <code>ErrClosed</code>. Receivers only observe <code>ErrClosed</code> (after draining) when <em>every</em> sender has been closed. Idempotent. Intended to be called by the producer goroutine that owns this sender — mpmc does not synchronize concurrent callers on the same sender handle.</dd>
</dl>

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
  <dd>Blocks until a value is available to this receiver. Returns the next value in the shared FIFO, or <code>ErrClosed</code> if the buffer is empty and every sender has closed, this receiver is closed, or the hub has been closed. Each enqueued value is delivered to exactly one receiver; competing receivers are scheduled by the Go runtime.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrNotReady</code> if no sender has yet been registered, <code>ErrEmpty</code> if the buffer is empty but at least one sender is still open, or <code>ErrClosed</code> if the buffer is empty and all senders are closed (or this receiver/the hub is closed).</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close this receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. The channel is shared across all receivers in the same hub — values delivered on it count against the single shared queue, so two receivers selecting on <code>Chan()</code> simultaneously still see each value only once. Closed when every sender has closed (directly or via <code>hub.Close()</code>) and the buffer drains. Closing <em>this</em> receiver does <em>not</em> close the channel; use <code>Recv</code>/<code>TryRecv</code> if you need that signal. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this receiver only. Other receivers and the senders are unaffected — they continue to consume and produce. Subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls on this handle return <code>ErrClosed</code>. Senders observe <code>ErrClosed</code> once <em>every</em> receiver has been closed (or the hub itself has been closed). Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Multi producer / multi consumer</dt>
  <dd>Any number of goroutines may each hold their own <code>*Sender[T]</code> (obtained from <code>hub.Sender()</code>) and call <code>Send</code>/<code>Close</code> on it; any number may each hold their own <code>*Receiver[T]</code> (obtained from <code>hub.Receiver()</code>) and call <code>Recv</code>/<code>Close</code> on it. The implementation does not synchronize concurrent callers on the <em>same</em> sender or receiver handle — call <code>hub.Sender()</code> once per producer goroutine and <code>hub.Receiver()</code> once per worker.</dd>
  <dt>Each value to exactly one receiver</dt>
  <dd>Values are <em>not</em> broadcast. A <code>Send</code> deposits one value into the shared queue, and the next <code>Recv</code> on any receiver removes it. Choice of receiver is non-deterministic and not guaranteed to be fair — it follows Go's channel-receive scheduling.</dd>
  <dt>FIFO across the queue, not across producers or consumers</dt>
  <dd>The queue itself preserves the order in which sends arrive at the underlying channel. Sends from a single producer remain in order with respect to each other, but the relative ordering of sends from <em>different</em> producers depends on scheduling. Any single receiver only sees a subsequence of the sends — interleaved with the work other receivers grabbed.</dd>
  <dt>Empty-hub gating</dt>
  <dd>A freshly constructed hub has neither senders nor receivers registered. <code>Send</code> blocks until <code>hub.Receiver()</code> has been called at least once; <code>Recv</code> blocks until <code>hub.Sender()</code> has been called at least once and a value has been sent. The implicit-close rules below only apply after each side has had at least one registration.</dd>
  <dt>All senders closed ⇒ receivers drain, then see ErrClosed</dt>
  <dd>Once at least one sender has been registered, every receiver observes <code>ErrClosed</code> when both (a) every sender obtained from <code>hub.Sender()</code> has been closed and (b) the buffer is empty. If you spawn N producers, you must close all N — a forgotten <code>Close</code> on any one of them leaves receivers waiting forever for an EOF that never arrives.</dd>
  <dt>All receivers closed ⇒ senders see ErrClosed</dt>
  <dd>Once at least one receiver has been registered, every sender's next <code>Send</code>/<code>TrySend</code>/<code>SendContext</code> returns <code>ErrClosed</code> if every receiver has been closed, and any buffered values are abandoned for <code>Recv</code>-style callers. This is how senders notice that nobody is left to do the work.</dd>
  <dt>Backpressure</dt>
  <dd>A bounded buffer applies natural backpressure: when full, <code>Send</code> blocks until some receiver makes room. Use <code>capacity == 0</code> for strict rendezvous handoff with no buffering.</dd>
  <dt>Hub close-all</dt>
  <dd><code>Hub.Close()</code> calls <code>Close</code> on every live sender and every live receiver. Recv-style callers see <code>ErrClosed</code> immediately; <code>Chan</code> consumers drain remaining values before seeing channel-closed.</dd>
</dl>

#### broadcast

A single-producer fan-out channel backed by a fixed-size ring buffer.
One `Sender` publishes values to any number of `Receiver`s, with every
value delivered to *every* live receiver (unlike `spmc`/`mpmc`, which
deliver each value to one receiver). The ring is bounded; slow receivers
don't block the sender — instead, the sender overwrites the oldest unread
slot and the lagging receiver gets a single [`gochan.ErrLagged`](#errors)
on its next `Recv` (telling it how many values it missed) before resuming
from the oldest still-buffered value.

Typical uses: event-stream fan-out (one producer, many listeners),
configuration-change notifications, market-data ticks distributed to many
strategies, WebSocket / SSE push systems where slow clients must not
back up the publisher.

Unlike `spmc`/`mpmc`, every receiver sees every value (unless there's lag).
Unlike `watch`, values are buffered (you see the last N, not just the
most recent).

**Constructor**

```go
func New[T any](capacity int) *Hub[T]
```

<dl>
  <dt><code>New[T](capacity)</code></dt>
  <dd>Creates a fresh broadcast hub backed by a ring buffer of the given <code>capacity</code>. <code>capacity &lt;= 0</code> panics: a ring of size 0 cannot hold a value across a sender→receiver handoff without blocking the sender, which contradicts the package's non-blocking promise. Pick a capacity that comfortably exceeds the burst size you expect between the sender and the slowest acceptable receiver — exceed it and that receiver will see <code>ErrLagged</code>.</dd>
</dl>

A freshly constructed broadcast hub has no receivers. Unlike <code>spmc</code> and <code>mpmc</code>, <code>Send</code> does <em>not</em> wait for a receiver to register — broadcast is "fire and forget" and the ring buffer accepts writes regardless of subscribers. Values written before the first <a href="#broadcast-receiver"><code>hub.Receiver()</code></a> call are not delivered to anyone: new receivers start at the current write position and only see values published after their registration. <code>ErrNotReady</code> is not produced by this package.

**Hub**

```go
func (h *Hub[T]) Sender()   gochan.Sender[T]
func (h *Hub[T]) Receiver() gochan.Receiver[T]
func (h *Hub[T]) Close()
```

<dl>
  <dt><code>Sender() gochan.Sender[T]</code></dt>
  <dd>Returns the singleton send-side handle. Repeated calls return the same handle. The handle is safe to share across goroutines — <code>Send</code>, <code>TrySend</code>, <code>SendContext</code>, and <code>Close</code> may all be called concurrently from any number of publishers. After the hub has been closed the returned handle reports <code>ErrClosed</code> on use.</dd>
  <dt id="broadcast-receiver"><code>Receiver() gochan.Receiver[T]</code></dt>
  <dd>Returns a new subscriber bound to the ring. The receiver's read position is set to the sender's current write position, so it sees only values published <em>after</em> this call — historical values still in the ring are not replayed. Each receiver has its own independent <code>Close</code> state and lag accounting. Safe to call concurrently. If the hub has been closed the returned handle is pre-closed and reports <code>ErrClosed</code> on use.</dd>
  <dt><code>Close()</code></dt>
  <dd>Equivalent to calling <code>Close</code> on the sender and on every live receiver. Receivers may still drain values that were written before the hub closed (subject to lag); subsequent <code>Recv</code> returns <code>ErrClosed</code> after the receiver catches up. Future <code>Sender</code> calls return the (now-closed) singleton; future <code>Receiver</code> calls return pre-closed handles. Idempotent.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt><code>Send(v) error</code></dt>
  <dd>Publishes <code>v</code> to the ring and returns immediately. <code>Send</code> never blocks: if the ring is full of unread values, the oldest unread slot is overwritten and the receiver(s) holding that slot will see <code>ErrLagged</code> on their next <code>Recv</code>. Returns <code>ErrClosed</code> if the sender or hub has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Pressure-aware variant. Returns <code>ErrFull</code> — <em>without writing</em> — if publishing the value would evict an unread value from at least one live receiver. Returns <code>ErrClosed</code> if closed. Returns <code>nil</code> on a successful write. <code>TrySend</code> is the entry point for senders that want to observe subscriber lag (for metrics, back-pressure, or self-throttling) or implement a drop-newest policy on top of the package's default drop-oldest behavior — see <a href="#broadcast-patterns">Patterns</a> below.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Returns <code>ctx.Err()</code> if <code>ctx</code> is already cancelled; otherwise identical to <code>Send</code>. Because <code>Send</code> never blocks, there is nothing for the context to interrupt mid-call — this method exists for interface symmetry with the rest of the library.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the sender. Already-published values remain in the ring and remain receivable (subject to lag) until each receiver catches up. Further <code>Send</code> / <code>TrySend</code> / <code>SendContext</code> calls return <code>ErrClosed</code>. Idempotent.</dd>
</dl>

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
  <dd>Blocks until the next value is available at this receiver's position, the sender has closed and this receiver has caught up, or this receiver/the hub is closed. Returns the next value, or one of:
    <ul>
      <li><code>gochan.ErrLagged</code> — the receiver fell more than <em>capacity</em> values behind the sender; some values were overwritten. The error carries <code>Missed</code> — the number of values dropped before the receiver caught up. The receiver's position is reset to the oldest still-buffered value; the next <code>Recv</code> resumes from there. The receiver is still usable.</li>
      <li><code>gochan.ErrClosed</code> — the sender or hub has closed and this receiver has already drained everything still in the ring at or after its position.</li>
    </ul>
  </dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is available at this receiver's position; <code>ErrEmpty</code> if the receiver is caught up to the sender; <code>gochan.ErrLagged</code> if the receiver has fallen behind (same semantics as <code>Recv</code>); or <code>ErrClosed</code> if the receiver/hub is closed.</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code> but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. A ready value or <code>ErrLagged</code> is preferred over a cancelled context.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns a per-receiver native channel that yields successive values in order. The channel is fed by the receiver and silently advances past lagged values — <code>ErrLagged</code> cannot be reported through a plain <code>&lt;-chan T</code>, so the channel pretends the dropped values never existed (i.e., the consumer sees the same value sequence as if it had called <code>Recv</code> in a loop and ignored <code>ErrLagged</code>). Use <code>Recv</code> / <code>TryRecv</code> / <code>RecvContext</code> instead if you need to observe lag. The channel is closed when the sender has closed and this receiver has drained the ring. Each receiver has its own channel; repeated calls on the same receiver return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this receiver only. Other receivers and the sender are unaffected. Subsequent <code>Recv</code> / <code>TryRecv</code> / <code>RecvContext</code> calls return <code>ErrClosed</code>. Idempotent. Closing the last receiver does <em>not</em> close the sender — broadcast lets the sender keep publishing into the ring with no subscribers (matches the "fire and forget" model).</dd>
</dl>

**Semantics**

<dl>
  <dt>Shared singleton sender, fan-out delivery</dt>
  <dd>The send-side handle returned by <code>hub.Sender()</code> is a singleton that is safe to share across goroutines: any number of publishers may call <code>Send</code> / <code>TrySend</code> / <code>SendContext</code> / <code>Close</code> concurrently on the same handle. Any number of goroutines may each hold their own <code>*Receiver[T]</code> (obtained from <code>hub.Receiver()</code>) and call <code>Recv</code>/<code>Close</code> on it; the implementation does not synchronize concurrent callers on the <em>same</em> receiver handle — call <code>hub.Receiver()</code> once per subscriber. Every value goes to <em>every</em> live receiver — this is fan-out, not load distribution. Use <code>spmc</code> or <code>mpmc</code> if you want each value delivered to exactly one consumer.</dd>
  <dt>Drop-oldest, with sender-observable pressure</dt>
  <dd>The ring buffer holds at most <em>capacity</em> values. When <code>Send</code> wraps around onto a slot still holding an unread value, the unread value is overwritten and the affected receiver(s) see <code>ErrLagged</code> on their next <code>Recv</code>. <code>TrySend</code> exposes the same condition to the publisher: it returns <code>ErrFull</code> and refuses to write when an overwrite would occur, letting publishers self-throttle or drop-newest if they prefer.</dd>
  <dt>Late subscribers see only future values</dt>
  <dd>A <code>Receiver</code> obtained via <code>hub.Receiver()</code> starts at the sender's current write position. Values published before registration are not replayed. To make a value durable across a subscribe boundary, publish it again after subscription completes.</dd>
  <dt>Sender Send never blocks</dt>
  <dd>By design, <code>Send</code> always returns immediately (success, overwrite, or <code>ErrClosed</code>). This is the inverse of the queue-style packages, where <code>Send</code> blocks under backpressure. If you want backpressure here, use <code>TrySend</code> + a back-off loop, or use <code>mpmc</code> instead.</dd>
  <dt>No empty-hub gating</dt>
  <dd>Unlike <code>spmc</code> / <code>mpmc</code>, broadcast does not block <code>Send</code> on the first <code>Hub.Receiver()</code> call. Values published with no subscribers are written to the ring but never delivered — subsequent subscribers start at "now" and don't see them. This package therefore never returns <code>ErrNotReady</code>.</dd>
  <dt>Hub close-all</dt>
  <dd><code>Hub.Close()</code> closes the sender and every live receiver. Recv-style callers can drain anything they had not yet consumed at the time of close (subject to lag) before seeing <code>ErrClosed</code>; <code>Chan</code> consumers see the channel close after their drain. The sender's view of close is immediate.</dd>
</dl>

<h5 id="broadcast-patterns">Patterns</h5>

Fire-and-forget telemetry — publisher doesn't care about lag, subscribers handle it:

```go
hub := broadcast.New[Metric](1024)

tx := hub.Sender()
go func() {
    for m := range metricStream {
        tx.Send(m) // never blocks; overwrites on wrap
    }
    tx.Close()
}()

rx := hub.Receiver().(*broadcast.Receiver[Metric])
for {
    m, err := rx.Recv()
    var lagged gochan.ErrLagged
    switch {
    case errors.As(err, &lagged):
        log.Warn("dropped metrics", "missed", lagged.Missed)
        continue
    case errors.Is(err, gochan.ErrClosed):
        return
    }
    process(m)
}
```

Sender-side observability — publisher tracks subscriber lag without changing delivery:

```go
for state := range stateStream {
    if err := tx.TrySend(state); errors.Is(err, gochan.ErrFull) {
        metrics.Inc("broadcast.subscriber_lagging")
    }
    tx.Send(state) // commit anyway — drop-oldest is fine for this stream
}
```

User-built drop-newest — preserve older values when the ring is full:

```go
for evt := range events {
    if err := tx.TrySend(evt); errors.Is(err, gochan.ErrFull) {
        droppedCount.Add(1)
        continue // skip the new value; keep older ones in the ring
    }
}
```

Self-throttling publisher — slow down when subscribers lag:

```go
for v := range source {
    for tx.TrySend(v) != nil {
        time.Sleep(backoff)
    }
}
```

#### watch
