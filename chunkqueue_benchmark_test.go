package chunkqueue_test

import (
	"context"
	"sync"
	"testing"

	"github.com/onur1/chunkqueue"
)

func BenchmarkPush(b *testing.B) {
	q := chunkqueue.NewChunkQueue[int](1024)
	ctx := context.Background()

	// Start a consumer to keep the queue from blocking
	go func() {
		for {
			_, err := q.Pop(ctx, 10)
			if err == chunkqueue.ErrQueueClosed {
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Push(ctx, i); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}

func BenchmarkPop(b *testing.B) {
	q := chunkqueue.NewChunkQueue[int](1024)
	ctx := context.Background()

	// Pre-fill the queue
	for i := 0; i < 1024; i++ {
		if err := q.Push(ctx, i); err != nil {
			b.Fatalf("Pre-fill Push failed: %v", err)
		}
	}
	q.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Pop(ctx, 10) // Pop chunks of size 10
		if err != nil && err != chunkqueue.ErrQueueClosed {
			b.Fatalf("Pop failed: %v", err)
		}
	}
}

func BenchmarkConcurrentPushPop(b *testing.B) {
	q := chunkqueue.NewChunkQueue[int](1024)
	ctx := context.Background()

	const producers = 4
	const consumers = 4

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	// Start producers
	producerWg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer producerWg.Done()
			for i := 0; i < b.N/producers; i++ {
				if err := q.Push(ctx, p*1000+i); err == chunkqueue.ErrQueueClosed {
					return
				}
			}
		}(p)
	}

	// Start consumers
	consumerWg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer consumerWg.Done()
			for {
				_, err := q.Pop(ctx, 10)
				if err == chunkqueue.ErrQueueClosed {
					return
				}
			}
		}()
	}

	// Wait for producers to finish, then close the queue
	producerWg.Wait()
	q.Close()

	// Wait for consumers to finish
	consumerWg.Wait()
}

func BenchmarkChannel(b *testing.B) {
	ch := make(chan int, 1024)
	ctx := context.Background()

	// Start a consumer to keep the channel from blocking
	go func() {
		for range ch {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case <-ctx.Done():
			return
		case ch <- i:
		}
	}
}
