package chunkqueue_test

import (
	"testing"
	"time"

	"github.com/onur1/chunkqueue"
)

func BenchmarkAdd(b *testing.B) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunkQueue.Add(i)
	}
}

func BenchmarkAddBatch(b *testing.B) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	batch := make([]int, 100)
	for i := 0; i < 100; i++ {
		batch[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunkQueue.AddBatch(batch)
	}
}

func BenchmarkReadChunk(b *testing.B) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	go func() {
		for i := 0; i < b.N; i++ {
			chunkQueue.Add(i)
		}
		chunkQueue.Close()
	}()

	b.ResetTimer()
	for {
		_, ok := chunkQueue.ReadChunk()
		if !ok {
			break
		}
	}
}

func BenchmarkConcurrentAddAndRead(b *testing.B) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	// Producer
	go func() {
		for i := 0; i < b.N; i++ {
			chunkQueue.Add(i)
			time.Sleep(time.Microsecond * 10)
		}
		chunkQueue.Close()
	}()

	// Consumer
	b.ResetTimer()
	for {
		_, ok := chunkQueue.ReadChunk()
		if !ok {
			break
		}
	}
}
