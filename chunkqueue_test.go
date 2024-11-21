package chunkqueue_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/onur1/chunkqueue"
)

func TestPushPopBasic(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](16)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Push some items
	for i := 1; i <= 5; i++ {
		err := q.Push(ctx, i)
		if err != nil {
			t.Errorf("Unexpected error pushing item: %v", err)
		}
	}

	// Pop items in batches
	for i := 0; i < 3; i++ {
		batch, err := q.Pop(ctx, 2)
		if err != nil {
			t.Errorf("Unexpected error popping items: %v", err)
		}
		fmt.Printf("Popped batch: %v\n", batch)
	}
}

func TestPushPopWithContextCancel(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](16)

	ctx, cancel := context.WithCancel(context.Background())

	// Push some items
	for i := 1; i <= 5; i++ {
		err := q.Push(ctx, i)
		if err != nil {
			t.Errorf("Unexpected error pushing item: %v", err)
		}
	}
	cancel()

	// Pop should be canceled due to context timeout
	_, err := q.Pop(ctx, 2)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context cancellation error, got: %v", err)
	}
}

func TestPushPopWithContextTimeout(t *testing.T) {
	queue := chunkqueue.NewChunkQueue[int](16)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	// Producer: Simulate pushing items into the queue
	go func() {
		defer wg.Done()
		for i := 1; i <= 10; i++ {
			err := queue.Push(ctx, i)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected context deadline exceeded error, got: %v", err)
				}
				return
			}
			time.Sleep(50 * time.Millisecond) // Simulate production delay
		}
	}()

	wg.Add(1)
	// Consumer: Simulate requesting batches from the queue
	go func() {
		defer wg.Done()
		for {
			time.Sleep(200 * time.Millisecond) // Simulate production delay
			batch, err := queue.Pop(ctx, 3)    // Fetch up to 3 items at a time
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected context deadline exceeded error, got: %v", err)
				}
				return
			}
			if ok := slices.Equal(batch, []int{1, 2, 3}); !ok {
				t.Errorf("Batch value not valid: %q", batch)
			}
		}
	}()

	// Let the program run until context expires
	<-ctx.Done()

	wg.Wait()
}

func TestPushPopClosedQueue(t *testing.T) {
	q := chunkqueue.NewChunkQueue[string](16)

	// Close the queue
	q.Close()

	// Close twice actually to cover q.closed == true
	q.Close()

	// Push should fail due to closed queue
	err := q.Push(context.Background(), "item")
	if err != chunkqueue.ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed when pushing to closed queue, got: %v", err)
	}

	// Pop should fail due to closed queue
	_, err = q.Pop(context.Background(), 2)
	if err != chunkqueue.ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed when popping from closed queue, got: %v", err)
	}
}

func TestPopEmptyQueue(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](16)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Pop from an empty queue should wait
	ch := make(chan struct{})
	go func() {
		_, err := q.Pop(ctx, 2)
		if err != nil {
			t.Errorf("Unexpected error popping from empty queue: %v", err)
		}
		ch <- struct{}{}
	}()

	// Simulate pushing an item after a short delay
	time.Sleep(50 * time.Millisecond)
	err := q.Push(ctx, 1)
	if err != nil {
		t.Errorf("Unexpected error pushing item: %v", err)
	}

	// Wait for the pop operation to complete
	<-ch
}

func TestPopBatchSize(t *testing.T) {
	q := chunkqueue.NewChunkQueue[string](16)
	ctx := context.Background()

	// Push 3 items
	for i := 0; i < 3; i++ {
		err := q.Push(ctx, fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Fatalf("Unexpected error pushing item: %v", err)
		}
	}

	// Pop with a batch size of 2
	batch, err := q.Pop(ctx, 2)
	if err != nil {
		t.Fatalf("Unexpected error popping items: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("Expected batch size of 2, got %d", len(batch))
	}

	// Pop the remaining item
	batch, err = q.Pop(ctx, 2)
	if err != nil {
		t.Fatalf("Unexpected error popping item: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("Expected batch size of 1, got %d", len(batch))
	}
}

func TestLargeVolumePushPop(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](1000000)
	ctx := context.Background()

	const itemCount = 1_000_000
	go func() {
		for i := 0; i < itemCount; i++ {
			_ = q.Push(ctx, i)
		}
		q.Close()
	}()

	popped := 0
	for {
		batch, err := q.Pop(ctx, 1000)
		if err == chunkqueue.ErrQueueClosed {
			break
		}
		popped += len(batch)
	}

	if popped != itemCount {
		t.Errorf("Expected to pop %d items, got %d", itemCount, popped)
	}
}

func TestConcurrentAccess(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](300)
	ctx := context.Background()
	const producerCount, consumerCount, itemsPerProducer = 3, 2, 100

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	// Producers
	for p := 0; p < producerCount; p++ {
		producerWg.Add(1)
		go func(p int) {
			defer producerWg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				_ = q.Push(ctx, p*itemsPerProducer+i)
			}
		}(p)
	}

	// Consumers
	popped := make(chan int, producerCount*itemsPerProducer)
	for c := 0; c < consumerCount; c++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for {
				batch, err := q.Pop(ctx, 10)
				if err == chunkqueue.ErrQueueClosed {
					return
				}
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				for _, item := range batch {
					popped <- item
				}
			}
		}()
	}

	// Close the queue after all producers are done
	go func() {
		producerWg.Wait() // Wait for all producers to finish
		q.Close()         // Close the queue
	}()

	// Wait for all consumers to finish
	consumerWg.Wait()
	close(popped) // Close the popped channel after consumers are done

	// Verify all items are consumed
	seen := make(map[int]bool)
	for item := range popped {
		if seen[item] {
			t.Errorf("Duplicate item: %d", item)
		}
		seen[item] = true
	}

	expectedItems := producerCount * itemsPerProducer
	if len(seen) != expectedItems {
		t.Errorf("Expected %d unique items, got %d", expectedItems, len(seen))
	}
}

func TestCloseDuringPushPop(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](16)
	ctx := context.Background()

	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(10 * time.Millisecond)
			_ = q.Push(ctx, i)
		}
	}()

	go func() {
		time.Sleep(100 * time.Millisecond)
		q.Close()
	}()

	for {
		batch, err := q.Pop(ctx, 5)
		if err == chunkqueue.ErrQueueClosed {
			break
		}
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		t.Log("Popped batch:", batch)
	}
}

func TestPushWhenQueueClosedMidWait(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](2)
	ctx := context.Background()

	var wg sync.WaitGroup

	// Start a producer that will block due to the queue being full
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := q.Push(ctx, 1, 2, 3) // Push more items than the queue capacity
		if err != chunkqueue.ErrQueueClosed {
			t.Errorf("Expected ErrQueueClosed when pushing to closed queue, got: %v", err)
		}
	}()

	// Allow some time for the producer to block
	time.Sleep(100 * time.Millisecond)

	// Close the queue while the producer is waiting
	q.Close()

	// Wait for the producer to finish
	wg.Wait()
}

func TestWrapAround(t *testing.T) {
	q := chunkqueue.NewChunkQueue[int](4)
	ctx := context.Background()

	// Fill the queue
	for i := 0; i < 4; i++ {
		err := q.Push(ctx, i)
		if err != nil {
			t.Fatalf("Unexpected error pushing item: %v", err)
		}
	}

	// Pop two items
	batch, err := q.Pop(ctx, 2)
	if err != nil {
		t.Fatalf("Unexpected error popping items: %v", err)
	}
	if len(batch) != 2 || batch[0] != 0 || batch[1] != 1 {
		t.Errorf("Expected popped batch: [0, 1], got: %v", batch)
	}

	// Push a new item, causing wrap-around
	err = q.Push(ctx, 4)
	if err != nil {
		t.Fatalf("Unexpected error pushing item: %v", err)
	}

	// Pop the remaining items
	batch, err = q.Pop(ctx, 3)
	if err != nil {
		t.Fatalf("Unexpected error popping items: %v", err)
	}
	if len(batch) != 2 || batch[0] != 2 || batch[1] != 3 {
		t.Errorf("Expected popped batch: [2, 3], got: %v", batch)
	}
}
