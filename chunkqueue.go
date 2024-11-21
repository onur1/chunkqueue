package chunkqueue

import (
	"context"
	"errors"
	"sync"
)

// ErrQueueClosed indicates that the queue is closed and cannot accept new operations.
var ErrQueueClosed = errors.New("queue is closed")

// ChunkQueue is a thread-safe queue that supports pushing and popping items in chunks.
type ChunkQueue[T any] struct {
	cond     *sync.Cond
	buffer   *Ring[T] // Circular buffer
	head     int      // Points to the next item to be popped
	tail     int      // Points to the next available slot for pushing
	size     int      // Number of items currently in the buffer
	capacity int      // Maximum capacity of the buffer
	closed   bool
}

// NewChunkQueue creates a new instance of ChunkQueue with the specified capacity.
func NewChunkQueue[T any](capacity int) *ChunkQueue[T] {
	return &ChunkQueue[T]{
		cond:     sync.NewCond(&sync.Mutex{}),
		buffer:   NewRing[T](capacity),
		capacity: capacity,
	}
}

// Push adds items to the queue. Returns an error if the context is canceled or the queue is closed.
func (q *ChunkQueue[T]) Push(ctx context.Context, items ...T) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed { // Check if the queue is closed before proceeding
		return ErrQueueClosed
	}

	for _, item := range items {
		for q.size == q.capacity { // Wait if the buffer is full
			if q.closed {
				return ErrQueueClosed
			}
			q.cond.Wait()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			q.buffer.Put(q.tail, item)
			q.tail++
			q.size++
			q.cond.Signal() // Notify waiting goroutines
		}
	}
	return nil
}

// Pop removes and returns a chunk of items from the queue. Blocks if the queue is empty.
// Returns an error if the context is canceled or the queue is closed.
func (q *ChunkQueue[T]) Pop(ctx context.Context, size int) ([]T, error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for {
		if q.closed && q.size == 0 { // Return if the queue is closed and empty
			return nil, ErrQueueClosed
		}

		for q.size == 0 { // Wait if the buffer is empty
			if q.closed {
				return nil, ErrQueueClosed
			}
			q.cond.Wait()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Ensure size doesn't exceed remaining elements
			if size > q.size {
				size = q.size
			}

			// Calculate end index with boundary check
			end := (q.head + size) & q.buffer.mask
			if end < q.head { // Wrap-around case
				end = q.buffer.mask + 1
			}

			// Return a slice referencing the underlying buffer
			batch := q.buffer.buf[q.head:end]
			q.head = end
			q.size -= size

			// Notify producers that space is available
			q.cond.Signal()

			return batch, nil
		}
	}
}

// Close marks the queue as closed and wakes up all waiting goroutines.
func (q *ChunkQueue[T]) Close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.cond.Broadcast() // Notify all waiting goroutines
}
