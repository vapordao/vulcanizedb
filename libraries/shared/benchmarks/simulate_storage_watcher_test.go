package benchmarks

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This benchmark is a proof of concept to see if the `DiffRepository.GetNewDiffs` could work in parallel and if this would
// give us any performance gains. The idea is that we would have several `GetNewDiffs` workers which could fetch new diffs
// and add them to a channel. Then hopefully `StorageWatcher.transformDiffs` wouldn't have to wait for new diffs to transform
// because they'll already be queued up and waiting to transform.
// I haven't successfully wired this up yet - getting a deadlock, but hopefully this is a fixable issue.
// A couple blog posts that may be useful:
//  - https://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html
//  - https://blog.golang.org/pipelines

// without go routine for GetNewDiffs
func BenchmarkBasicNoGoRoutine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		transformDiffs()
	}
}

func transformDiffs() {
	ints := getNewDiffs()

	var total uint32
	for _, v := range ints {
		// sleep to simulate taking time to insert
		time.Sleep(time.Nanosecond)
		atomic.AddUint32(&total, v)
	}
}

func getNewDiffs() []uint32 {
	var c []uint32
	for i := uint32(0); i < 1000; i++ {
		c = append(c, i%2)
	}
	time.Sleep(time.Second)
	return c
}

//with a go routine
func BenchmarkBasicWithNoBuffer(b *testing.B) {
	withBuffer(b, 0)
}

func BenchmarkBasicWithBufferSizeOf1(b *testing.B) {
	withBuffer(b, 1)
}

func BenchmarkBasicWithBufferSizeEqualsToNumberOfWorker(b *testing.B) {
	withBuffer(b, 5)
}

func BenchmarkBasicWithBufferSizeExceedsNumberOfWorker(b *testing.B) {
	withBuffer(b, 25)
}


func withBuffer(b *testing.B, size int) {
	for i := 0; i < b.N; i++ {
		c := make(chan uint32, size)
		wg := sync.WaitGroup{}

		for w := 0; w < 1; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := uint32(0); i < 1000; i++ {
					c <- i % 2
				}
				time.Sleep(time.Second)
			}()
		}

		var total uint32
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, ok := <-c
				if !ok {
					break
				}

				time.Sleep(time.Nanosecond)
				atomic.AddUint32(&total, v)
			}
		}()

		wg.Wait()
	}
}
