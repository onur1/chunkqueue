package chunkqueue_test

import (
	"context"
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

func TestAddAndReadClosedQueue(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)
	chunkQueue.Close()

	err := chunkQueue.Add(1)
	if err != chunkqueue.ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed, got %v", err)
	}

	chunk, ok := chunkQueue.ReadChunk()
	if ok {
		t.Fatal("Expected no chunk from closed queue, but got one")
	}
	if chunk != nil {
		t.Fatalf("Expected no chunk, but got %v", chunk)
	}
}

func TestReadChunkWithContextCancel(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	ctx, cancel := context.WithCancel(context.Background())

	// Simulate adding items with a delay
	go func() {
		time.Sleep(time.Millisecond * 50)
		chunkQueue.AddBatch([]int{1, 2, 3, 4, 5})
	}()

	// Cancel the context before the item is added
	cancel()

	// Attempt to read a chunk
	chunk, ok, err := chunkQueue.ReadChunkWithContext(ctx)
	if err == nil {
		t.Fatal("Expected context cancellation error, got nil")
	}
	if ok {
		t.Fatal("Expected no chunk, but got a valid chunk")
	}
	if chunk != nil {
		t.Fatalf("Expected no chunk, but got %v", chunk)
	}
}

func TestReadChunkWithContextTimeout(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()

	// Simulate adding items after the timeout
	go func() {
		time.Sleep(time.Millisecond * 100)
		chunkQueue.AddBatch([]int{1, 2, 3, 4, 5})
	}()

	// Attempt to read a chunk
	chunk, ok, err := chunkQueue.ReadChunkWithContext(ctx)
	if err == nil || err != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded error, got %v", err)
	}
	if ok {
		t.Fatal("Expected no chunk, but got a valid chunk")
	}
	if chunk != nil {
		t.Fatalf("Expected no chunk, but got %v", chunk)
	}
}

func TestReadChunkWithContextSuccess(t *testing.T) {
	chunkQueue := chunkqueue.NewChunkQueue[int](5)

	ctx := context.Background()

	// Simulate adding items
	go func() {
		time.Sleep(time.Millisecond * 50)
		chunkQueue.AddBatch([]int{1, 2, 3, 4, 5})
	}()

	// Attempt to read a chunk
	chunk, ok, err := chunkQueue.ReadChunkWithContext(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !ok {
		t.Fatal("Expected a valid chunk, but got none")
	}
	expected := []int{1, 2, 3, 4, 5}
	if !equal(expected, chunk) {
		t.Errorf("Expected %v, got %v", expected, chunk)
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
