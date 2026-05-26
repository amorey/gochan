# gochan

*gochan is a small library containing multiple channel architectures for Go, inspired by Rust*

## Introduction

Go channels are extremely useful but they only ship with one type - mpmc (multiple-producer/multiple-consumer), buffered or un-buffered. This means that we often have to add higher level logic to our data structures in order to implement common patterns like single-shot, broadcasts and watches. Inspired by [`Rust channels`](https://doc.rust-lang.org/rust-by-example/std_misc/channels.html), this library adds seven specialized channel types to Go that you can use to implement common architectures that aren't provided by Go's built-in `chan` type:

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
tx := spmc.NewBounded[Job](128)
for i := 0; i < workers; i++ {
    rx := tx.Consumer()
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
rx := mpsc.NewBounded[Event](256)
for i := 0; i < n; i++ {
    s := rx.Producer()
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

## Design notes

Here are some design decisions to be aware of:

**Bounded by default**: Unbounded queues are a memory-safety footgun. Only `mpsc` offers an unbounded variant, and the doc comment warns about it. Everything else takes an explicit capacity.

**Queue-style channel capacity**: (`spsc`, `spmc`, `mpsc (bounded)`, `mpmc`), capacity behaves exactly like Go's buffered channels because the implementation *is* a buffered Go channel underneath. `NewBounded[T](0)` is a rendezvous channel — `Send` blocks until a `Recv` is ready. `NewBounded[T](n)` allows `n` queued values before `Send` blocks.

**`mpsc.NewUnbounded` grows as needed**: Use this when bursts are unavoidable and bounded back-pressure would deadlock you — but watch for memory growth if producers can outrun the consumer indefinitely.

**Broadcast and watch use Subscribe() semantics**: The `Sender` returned by `broadcast` and `watch` constructors exposes a `Subscribe()` method which returns a `Receiver`. Calling `Close()` on a subscriber receiver removes only that receiver from the subscriber set; the sender and other subscribers are unaffected. This lets listeners come and go independently of the sender's lifecycle.

**Broadcast uses a ring buffer**: Slow receivers don't block the sender — they get `ErrLagged` and skip forward.


## API

### Constructors

Each package exposes one or two constructors, some with explicit `capacity` arguments:

| Constructor              | Capacity arg | `0` allowed?     | Semantics                                    |
| ------------------------ | ------------ | ---------------- | -------------------------------------------- |
| `oneshot.New[T]()`       | —            | —                | Single value, single delivery                |
| `spsc.NewBounded[T](n)`  | yes          | yes (rendezvous) | FIFO queue, one sender and one receiver      |
| `spmc.NewBounded[T](n)`  | yes          | yes (rendezvous) | One sender, items load-balanced to receivers spawned via `tx.Consumer()` |
| `mpsc.NewBounded[T](n)`  | yes          | yes (rendezvous) | Fan-in to one receiver from many senders spawned via `rx.Producer()` |
| `mpsc.NewUnbounded[T]()` | —            | —                | Same shape as `NewBounded`, but grows without bound |
| `mpmc.NewBounded[T](n)`  | yes          | yes (rendezvous) | Shared queue, any sender to any receiver     |
| `broadcast.New[T](n)`    | yes          | no (panics)      | Ring buffer size; overwrites on lag          |
| `watch.New[T](initial)`  | —            | —                | Single slot, always holds the latest value   |

`broadcast` and `watch` return only a sender; receivers are obtained via `tx.Subscribe()`. This matches their fan-out nature — receivers come and go independently of the sender's lifecycle.

### Common interface

Every channel type implements the same two interfaces to make switching between architectures easier:

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

### Errors

A small set of sentinel errors is shared across all packages:

```go
var ErrClosed = errors.New("chans: channel closed")
var ErrFull   = errors.New("chans: channel full")
var ErrEmpty  = errors.New("chans: channel empty")

type ErrLagged struct{ Skipped uint64 }  // broadcast only
```

`ErrLagged` is specific to `broadcast` and signals that a slow receiver has fallen behind the ring buffer. The receiver is still usable and will resume from the oldest still-buffered value.

### Packages

#### oneshot

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
</dl>

#### spsc

A single-producer, single-consumer FIFO queue. One `Sender` feeds values in
order to one `Receiver`. Capacity behaves exactly like a Go buffered channel:
`NewBounded[T](0)` is a rendezvous channel, `NewBounded[T](n)` allows `n`
queued values before `Send` blocks.

Typical uses: streaming pipelines between two cooperating goroutines, a
producer/consumer stage in a larger dataflow, decoupling a fast producer from
a slow consumer with a fixed-size buffer.

**Constructor**

```go
func NewBounded[T any](capacity int) (*Sender[T], *Receiver[T])
```

<dl>
  <dt><code>NewBounded[T](capacity)</code></dt>
  <dd>Creates a fresh spsc pair backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where <code>Send</code> blocks until a matching <code>Recv</code> is ready. <code>capacity &lt; 0</code> panics.</dd>
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
  <dd>Blocks until a value is available. Returns the next value in FIFO order, or <code>ErrClosed</code> if the buffer is empty and the sender has closed (or the receiver itself is closed).</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrEmpty</code> if the buffer is empty but the sender is still open, or <code>ErrClosed</code> if the buffer is empty and closed.</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close the receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. The channel is closed when the sender closes and the buffer drains. Closing the receiver does <em>not</em> close this channel — use <code>Recv</code>/<code>TryRecv</code> (which observe receiver-close) if you need that signal. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the receiver. Pending or future <code>Send</code> calls return <code>ErrClosed</code>, and subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls return <code>ErrClosed</code>. Any values still buffered are abandoned (no further <code>Recv</code> will see them). Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Single producer / single consumer</dt>
  <dd>Exactly one goroutine should call <code>Send</code>/<code>Close</code> on the sender and exactly one goroutine should call <code>Recv</code>/<code>Close</code> on the receiver. The implementation does not synchronize multiple concurrent callers on the same side.</dd>
  <dt>FIFO ordering</dt>
  <dd>Values are received in the exact order they were sent.</dd>
  <dt>Drain on sender close</dt>
  <dd>Closing the sender does not discard already-buffered values; the receiver drains them in order before observing <code>ErrClosed</code>. Closing the <em>receiver</em>, by contrast, drops anything still in the buffer.</dd>
  <dt>Backpressure</dt>
  <dd>A bounded buffer applies natural backpressure: when full, <code>Send</code> blocks until the consumer makes room. Use <code>capacity == 0</code> for strict rendezvous handoff with no buffering.</dd>
</dl>

#### spmc

A single-producer, multi-consumer FIFO queue. One `Sender` feeds values into
a bounded buffer that is drained by any number of `Receiver`s, with each
value delivered to exactly one receiver. Capacity behaves exactly like a Go
buffered channel: `NewBounded[T](0)` is a rendezvous channel,
`NewBounded[T](n)` allows `n` queued values before `Send` blocks.

Typical uses: distributing work items to a pool of workers from a single
dispatcher goroutine, parallelizing a CPU-bound pipeline stage, fanning a
single input stream out across N consumers without duplication.

Unlike `broadcast`, every value goes to *one* receiver, not all of them —
`spmc` is load distribution, not fan-out.

**Constructor**

```go
func NewBounded[T any](capacity int) *Sender[T]
```

<dl>
  <dt><code>NewBounded[T](capacity)</code></dt>
  <dd>Creates a fresh spmc sender backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where <code>Send</code> blocks until some receiver is ready. <code>capacity &lt; 0</code> panics. The constructor returns only a sender; receivers are obtained from it via <a href="#spmc-consumer"><code>tx.Consumer()</code></a>. A freshly constructed spmc has no receivers, so <code>Send</code> will block (or <code>TrySend</code> will report <code>ErrFull</code>) until at least one <code>Consumer</code> is registered.</dd>
</dl>

**Sender**

```go
func (tx *Sender[T]) Consumer() *Receiver[T]
func (tx *Sender[T]) Send(v T) error
func (tx *Sender[T]) TrySend(v T) error
func (tx *Sender[T]) SendContext(ctx context.Context, v T) error
func (tx *Sender[T]) Close()
```

<dl>
  <dt id="spmc-consumer"><code>Consumer() *Receiver[T]</code></dt>
  <dd>Returns a new receiver bound to the shared queue. Use this to add workers to the consumer pool. Each returned receiver has its own independent <code>Close</code> state but shares the queue and sender-close signal with every other receiver. Safe to call concurrently. If every previously-registered receiver has already been closed (so the sender has already observed <code>ErrClosed</code>), the returned receiver is pre-closed.</dd>
  <dt><code>Send(v) error</code></dt>
  <dd>Enqueues <code>v</code>, blocking while the buffer is full and no receiver is ready to consume it. Returns <code>ErrClosed</code> if the sender has been closed or every receiver has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrFull</code> if the buffer is full and no receiver is currently parked on a recv, <code>ErrClosed</code> if closed, or <code>nil</code> on success.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes the sender. Already-queued values remain receivable; receivers drain them in order before observing <code>ErrClosed</code>. Further <code>Send</code> calls return <code>ErrClosed</code>. Idempotent. Intended to be called by the single producer — spmc does not synchronize concurrent callers on the sender side (though <code>Consumer</code> is safe to call from any goroutine).</dd>
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
  <dd>Blocks until a value is available to this receiver. Returns the next value in the shared FIFO, or <code>ErrClosed</code> if the buffer is empty and the sender has closed, or this receiver is closed. Each enqueued value is delivered to exactly one receiver; competing receivers are scheduled by the Go runtime.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrEmpty</code> if the buffer is empty but the sender is still open, or <code>ErrClosed</code> if the buffer is empty and closed (or this receiver is closed).</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close this receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. The channel is shared across all receivers in the same spmc — values delivered on it count against the single shared queue, so two receivers selecting on <code>Chan()</code> simultaneously still see each value only once. Closed when the sender closes and the buffer drains. Closing this receiver does <em>not</em> close the channel; use <code>Recv</code>/<code>TryRecv</code> if you need that signal. Repeated calls return the same channel.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this receiver only. Other receivers and the sender are unaffected — they continue to consume and produce. Subsequent <code>Recv</code>/<code>TryRecv</code>/<code>RecvContext</code> calls on this handle return <code>ErrClosed</code>. The sender only observes <code>ErrClosed</code> when <em>every</em> receiver has been closed. Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Single producer / multi consumer</dt>
  <dd>Exactly one goroutine should call <code>Send</code>/<code>Close</code> on the sender. Any number of goroutines may each hold their own <code>*Receiver[T]</code> (obtained from <code>tx.Consumer()</code>) and call <code>Recv</code>/<code>Close</code> on it. The implementation does not synchronize concurrent callers on the <em>same</em> receiver handle — call <code>Consumer</code> once per worker.</dd>
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
</dl>

#### mpsc

A multiple-producer, single-consumer FIFO queue. Any number of `Sender` handles feed values into a shared queue drained by one `Receiver`. Two flavors are provided: a bounded buffer (`NewBounded`) that applies backpressure, and an unbounded buffer (`NewUnbounded`) that grows as needed.

Typical uses: fan-in of events from many worker goroutines into a single
aggregator, collecting results from a scatter of parallel tasks, funnelling
log/metric/event streams to one writer.

**Constructors**

```go
func NewBounded[T any](capacity int) *Receiver[T]
func NewUnbounded[T any]() *Receiver[T]
```

<dl>
  <dt><code>NewBounded[T](capacity)</code></dt>
  <dd>Creates a fresh mpsc receiver backed by a buffered Go channel of the given <code>capacity</code>. <code>capacity == 0</code> yields a rendezvous channel where every <code>Send</code> blocks until the receiver is ready. <code>capacity &lt; 0</code> panics. Bounded variants give you natural backpressure: when the buffer is full, producers wait.</dd>
  <dt><code>NewUnbounded[T]()</code></dt>
  <dd>Creates a fresh mpsc receiver with an unbounded internal queue. <code>Send</code> never blocks on capacity (only on closed state). Use this when bursts are unavoidable and bounded backpressure would deadlock you — but watch for memory growth if producers can outrun the consumer indefinitely. <code>TrySend</code> never returns <code>ErrFull</code>.</dd>
</dl>

Both constructors return only a receiver; senders are obtained from it via <a href="#mpsc-producer"><code>rx.Producer()</code></a>. A freshly constructed mpsc has no senders, so <code>Recv</code> will block (or <code>TryRecv</code> will report <code>ErrEmpty</code>) until at least one producer is registered and sends a value.

**Receiver**

```go
func (rx *Receiver[T]) Producer() *Sender[T]
func (rx *Receiver[T]) Recv() (T, error)
func (rx *Receiver[T]) TryRecv() (T, error)
func (rx *Receiver[T]) RecvContext(ctx context.Context) (T, error)
func (rx *Receiver[T]) Chan() <-chan T
func (rx *Receiver[T]) Close()
```

<dl>
  <dt id="mpsc-producer"><code>Producer() *Sender[T]</code></dt>
  <dd>Returns a new sender bound to the shared queue. Use this to add producers to the fan-in. Each returned sender has its own independent <code>Close</code> state but shares the queue and receiver-close signal with every other sender. Safe to call concurrently. If the receiver has been closed (or every previously-registered sender has been closed and the buffer has drained), the returned sender is pre-closed.</dd>
  <dt><code>Recv() (T, error)</code></dt>
  <dd>Blocks until a value is available. Returns the next value in FIFO order, or <code>ErrClosed</code> if the buffer is empty and every sender has closed, or this receiver is closed.</dd>
  <dt><code>TryRecv() (T, error)</code></dt>
  <dd>Non-blocking. Returns the next value if one is buffered, <code>ErrEmpty</code> if the buffer is empty but at least one sender is still open, or <code>ErrClosed</code> if the buffer is empty and all senders are closed (or the receiver is closed).</dd>
  <dt><code>RecvContext(ctx) (T, error)</code></dt>
  <dd>Like <code>Recv</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled first. Cancellation does <em>not</em> close the receiver; subsequent calls remain valid.</dd>
  <dt><code>Chan() &lt;-chan T</code></dt>
  <dd>Returns the underlying receive-only channel, suitable for use in <code>select</code>. Closed when every sender has closed and the buffer has drained. Closing the receiver does <em>not</em> close this channel — use <code>Recv</code>/<code>TryRecv</code> if you need that signal. Repeated calls return the same channel.</dd>
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
  <dd>Enqueues <code>v</code>. For bounded mpsc, blocks while the buffer is full; for unbounded mpsc, returns as soon as the value is appended. Returns <code>ErrClosed</code> if this sender has been closed or the receiver has been closed; on <code>ErrClosed</code> the value is dropped.</dd>
  <dt><code>TrySend(v) error</code></dt>
  <dd>Non-blocking. Returns <code>ErrFull</code> if a bounded buffer is full, <code>ErrClosed</code> if closed, or <code>nil</code> on success. For unbounded mpsc, <code>ErrFull</code> is never returned.</dd>
  <dt><code>SendContext(ctx, v) error</code></dt>
  <dd>Like <code>Send</code>, but returns <code>ctx.Err()</code> if <code>ctx</code> is cancelled before the value is enqueued. The value is dropped on cancellation. For unbounded mpsc, cancellation effectively only matters at entry, since the enqueue itself doesn't block.</dd>
  <dt><code>Close()</code></dt>
  <dd>Closes this sender only. Other senders continue to produce. Subsequent <code>Send</code>/<code>TrySend</code>/<code>SendContext</code> calls on this handle return <code>ErrClosed</code>. The receiver only observes <code>ErrClosed</code> (after draining) when <em>every</em> sender has been closed. Idempotent.</dd>
</dl>

**Semantics**

<dl>
  <dt>Multi producer / single consumer</dt>
  <dd>Any number of goroutines may each hold their own <code>*Sender[T]</code> (obtained from <code>rx.Producer()</code>) and call <code>Send</code>/<code>Close</code> on it. Exactly one goroutine should call <code>Recv</code>/<code>Close</code> on the receiver. The implementation does not synchronize concurrent callers on the <em>same</em> sender handle — call <code>Producer</code> once per producer goroutine.</dd>
  <dt>FIFO across the queue, not across producers</dt>
  <dd>The queue itself preserves the order in which sends arrive at the underlying channel, but the relative ordering of sends from <em>different</em> producers is not defined — it depends on scheduling. Sends from a single producer remain in order with respect to each other.</dd>
  <dt>All senders closed ⇒ receiver drains, then sees ErrClosed</dt>
  <dd>The receiver does not observe <code>ErrClosed</code> until both (a) every sender obtained from <code>Producer</code> has been closed and (b) the buffer is empty. If you spawn N producers, you must close all N — a forgotten <code>Close</code> on any one of them leaves the receiver waiting forever for an EOF that never arrives.</dd>
  <dt>Receiver close stops everything</dt>
  <dd>Closing the receiver immediately fails every pending and future <code>Send</code> across all senders with <code>ErrClosed</code> and abandons any buffered values.</dd>
  <dt>Bounded vs unbounded backpressure</dt>
  <dd><code>NewBounded</code> applies natural backpressure: when full, <code>Send</code> blocks until the consumer makes room. <code>NewUnbounded</code> trades that backpressure for memory growth — appropriate when bursts are bounded by upstream logic, inappropriate when producers can outrun the consumer indefinitely.</dd>
</dl>

#### mpmc

#### broadcast

#### watch
