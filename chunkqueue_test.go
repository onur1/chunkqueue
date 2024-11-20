package chunkqueue_test

import (
	"testing"
	"time"

	"github.com/onur1/chunkqueue"
)

func TestAddAndReadChunk(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	go func() {
		chunkQueue.Add(1)
		chunkQueue.Add(2)
		chunkQueue.Add(3)
		chunkQueue.Add(4)
		chunkQueue.Add(5)
		chunkQueue.Close()
	}()

	expectedChunks := [][]int{
		{1, 2, 3, 4, 5},
	}

	for _, expectedChunk := range expectedChunks {
		chunk, ok := chunkQueue.ReadChunk()
		if !ok {
			t.Fatalf("Expected chunk but got none")
		}
		if !equal(expectedChunk, chunk) {
			t.Errorf("Expected %v, got %v", expectedChunk, chunk)
		}
	}

	if chunk, ok := chunkQueue.ReadChunk(); ok {
		t.Errorf("Expected no more chunks, but got %v", chunk)
	}
}

func TestAddBatchAndReadChunk(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	go func() {
		chunkQueue.AddBatch([]int{1, 2, 3, 4, 5, 6, 7})
		chunkQueue.Close()
	}()

	expectedChunks := [][]int{
		{1, 2, 3, 4, 5},
		{6, 7},
	}

	for _, expectedChunk := range expectedChunks {
		chunk, ok := chunkQueue.ReadChunk()
		if !ok {
			t.Fatalf("Expected chunk but got none")
		}
		if !equal(expectedChunk, chunk) {
			t.Errorf("Expected %v, got %v", expectedChunk, chunk)
		}
	}

	if chunk, ok := chunkQueue.ReadChunk(); ok {
		t.Errorf("Expected no more chunks, but got %v", chunk)
	}
}

func TestConcurrentAddAndRead(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	go func() {
		for i := 1; i <= 10; i++ {
			chunkQueue.Add(i)
			time.Sleep(time.Millisecond * 10)
		}
		chunkQueue.Close()
	}()

	readCount := 0
	for {
		chunk, ok := chunkQueue.ReadChunk()
		if !ok {
			break
		}
		readCount += len(chunk)
	}

	if readCount != 10 {
		t.Errorf("Expected to read 10 items, but got %d", readCount)
	}
}

// Helper function to compare slices
func equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
