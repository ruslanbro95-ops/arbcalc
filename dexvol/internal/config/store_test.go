package config

import (
	"path/filepath"
	"sync"
	"testing"
)

// TestConcurrentGetAndUpdate mirrors the real process: the bot goroutine writes
// settings while the service's seal, price and backfill loops read them.
//
// Runtime carries a map, and Go aborts the process outright on a concurrent map
// read and write — so before this was guarded, one /windows command at the
// wrong instant could take the monitor down. Run under -race.
func TestConcurrentGetAndUpdate(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "state.json"))

	var wg sync.WaitGroup
	readers, writers := 4, 2
	wg.Add(readers + writers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < 2000; n++ {
				rt := s.Get()
				_ = rt.Windows[10]
				_ = rt.ThresholdPct
				_ = len(rt.Tokens)
			}
		}()
	}
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for n := 0; n < 2000; n++ {
				s.Update(func(rt *Runtime) {
					rt.Windows[10] = n%2 == 0
					rt.ThresholdPct = float64(n)
				})
			}
		}(i)
	}
	wg.Wait()
}

func TestGetReturnsADeepCopy(t *testing.T) {
	// A caller must not be able to reach into the store through the value it
	// was handed, or a read would quietly become a write.
	s := NewStore(filepath.Join(t.TempDir(), "state.json"))
	got := s.Get()

	got.Windows[10] = false
	got.ThresholdPct = 999

	fresh := s.Get()
	if !fresh.Windows[10] {
		t.Fatal("mutating the returned map changed the store")
	}
	if fresh.ThresholdPct == 999 {
		t.Fatal("mutating the returned struct changed the store")
	}
}
