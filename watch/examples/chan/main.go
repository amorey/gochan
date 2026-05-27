// watch/examples/chan demonstrates the Chan()-based API for a
// latest-value-only channel, with the subscriber composing Chan()
// with a cancel signal via select for graceful early shutdown.
//
// As with broadcast, Chan() on a watch receiver is a PRIVATE channel
// fed by a per-receiver goroutine. Receiver.Close() shuts that feeder
// down (and closes Chan()); Sender.Close() also closes Chan() after
// delivering the final value once. Always Close the receiver when you
// stop reading or the feeder will leak.
//
// Run:
//
//	go run ./watch/examples/chan
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

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

	cancel := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(cancel)
	}()

	// Subscribers: each gets its own Receiver and its own feeder. The
	// first value on Chan() is the seed Config — receivers always
	// bootstrap with the current value. Because each subscriber paces
	// itself, they may coalesce different intermediate versions.
	const subscribers = 3
	var wg sync.WaitGroup
	for id := 0; id < subscribers; id++ {
		rx := hub.Receiver()
		// Call Chan() here (not inside the goroutine) so the feeder is
		// started before the producer loop runs. The first delivery on
		// Chan() is whatever s.val is at the moment the feeder starts,
		// so if Chan() races against early Sends the subscriber may
		// bootstrap from v1 or v2 instead of the seed.
		ch := rx.Chan()
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer rx.Close() // stops the per-receiver feeder goroutine
			for {
				select {
				case cfg, ok := <-ch:
					if !ok {
						// Sender closed and feeder delivered the final value: done.
						return
					}
					// Simulate variable work to apply the config. If the producer
					// emits new values during this sleep, they coalesce — watch
					// only ever delivers the latest, so older versions are skipped.
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					fmt.Printf("sub %d apply: version=%d name=%q\n", id, cfg.Version, cfg.Name)
				case <-cancel:
					// Graceful early exit. Deferred rx.Close shuts the feeder.
					return
				}
			}
		}(id)
	}

	tx := hub.Sender()
	// Pause so subscribers observe the seed value before any Send races in.
	time.Sleep(20 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		if err := tx.Send(Config{Version: i, Name: fmt.Sprintf("v%d", i)}); err != nil {
			// ErrClosed here means the hub was torn down — stop producing.
			fmt.Println("send failed:", err)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Soft shutdown: subscriber observes the final Config once via Chan()
	// before the channel closes.
	tx.Close()

	wg.Wait()
	fmt.Println("done")
}
