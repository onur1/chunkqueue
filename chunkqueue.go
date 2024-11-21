// Package chunkqueue provides a thread-safe, generic queue with support for chunk-based reading.
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
	cond   *sync.Cond
	buffer []T
	closed bool
}

// NewChunkQueue creates a new instance of ChunkQueue.
func NewChunkQueue[T any]() *ChunkQueue[T] {
	return &ChunkQueue[T]{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Push adds items to the queue. Returns an error if the context is canceled or the queue is closed.
func (q *ChunkQueue[T]) Push(ctx context.Context, item ...T) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	select {
	case <-ctx.Done(): // Check if the context has been canceled
		return ctx.Err()
	default:
		if q.closed { // Check if the queue is closed
			return ErrQueueClosed
		}
		q.buffer = append(q.buffer, item...) // Add items to the buffer
		q.cond.Signal()                      // Notify waiting goroutines
	}

	return nil
}

// Pop removes and returns a chunk of items from the queue. Blocks if the queue is empty.
// Returns an error if the context is canceled or the queue is closed.
func (q *ChunkQueue[T]) Pop(ctx context.Context, size int) ([]T, error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for {
		select {
		case <-ctx.Done(): // Handle context cancellation
			return nil, ctx.Err()
		default:
			// If the buffer has items, return a batch
			if len(q.buffer) > 0 {
				if len(q.buffer) < size {
					size = len(q.buffer)
				}
				batch := q.buffer[:size]
				q.buffer = q.buffer[size:] // Remove retrieved items from buffer
				return batch, nil
			}

			// If the queue is closed and the buffer is empty, return ErrQueueClosed
			if q.closed {
				return nil, ErrQueueClosed
			}

			// Wait for items to be added if the queue is not closed
			q.cond.Wait()
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
