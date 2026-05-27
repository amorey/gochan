// watch/examples/recv demonstrates the Recv()-based API for a
// latest-value-only channel — the classic "current config" pattern.
//
// The hub is seeded with an initial Config; the producer publishes
// updates and intermediate updates that are overwritten before a
// receiver reads them are silently dropped. Each receiver's first
// Recv returns the current value without waiting, so subscribers
// bootstrap immediately.
//
// Graceful shutdown uses Sender.Close (the soft path): each live
// receiver sees the final value once before subsequent Recv calls
// return ErrClosed. This is in contrast to Hub.Close, which is hard
// tear-down (sender + every receiver) with no final-value drain.
//
// Run:
//
//	go run ./watch/examples/recv
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gochan"
	"github.com/amorey/gochan/watch"
)

type Config struct {
	Version int
	Name    string
}

func main() {
	hub := watch.New(Config{Version: 0, Name: "initial"})
	// hub.Close() is idempotent close-all and hard tear-down (skips the
	// final-value drain that tx.Close gives subscribers). Deferring is
	// still safe as a backstop because by the time it fires the soft
	// shutdown below has already completed.
	defer hub.Close()

	// Subscribers: each gets its own Receiver. The first Recv returns
	// the seed value immediately — there is no "empty" state. Because
	// each subscriber paces itself, they may coalesce different
	// intermediate versions.
	const subscribers = 3
	var wg sync.WaitGroup
	for id := 0; id < subscribers; id++ {
		rx := hub.Receiver()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer rx.Close()
			for {
				cfg, err := rx.Recv()
				if err != nil {
					if !errors.Is(err, gochan.ErrClosed) {
						fmt.Printf("sub %d recv error: %v\n", id, err)
					}
					return
				}
				// Simulate variable work to apply the config. If the producer
				// emits new values during this sleep, they coalesce — watch
				// only ever delivers the latest, so older versions are skipped.
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
				fmt.Printf("sub %d apply: version=%d name=%q\n", id, cfg.Version, cfg.Name)
			}
		}(id)
	}

	// Producer: publish several updates. If we publish faster than the
	// subscriber reads, intermediate values are dropped and only the
	// latest is observed — that is by design.
	tx := hub.Sender()
	// Pause so subscribers observe the seed value before any Send races in.
	time.Sleep(20 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		if err := tx.Send(Config{Version: i, Name: fmt.Sprintf("v%d", i)}); err != nil {
			// ErrClosed here means the hub was torn down — stop producing.
			fmt.Println("send failed:", err)
			break
		}
		time.Sleep(20 * time.Millisecond) // give the subscribers a chance to see each one
	}

	// Soft shutdown: Sender.Close lets the subscriber observe the final
	// value once before its next Recv returns ErrClosed. If we wanted
	// hard tear-down (no final-value delivery) we'd call hub.Close().
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
