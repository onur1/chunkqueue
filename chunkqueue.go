package chunkqueue

import (
	"errors"
	"sync"
)

// ChunkQueue manages a thread-safe queue with chunked reads.
type ChunkQueue[T any] struct {
	maxChunk int
	cond     *sync.Cond
	mu       sync.Mutex
	data     []T
	isClosed bool
}

// NewChunkQueue initializes a new chunking queue with the specified chunk size.
func NewChunkQueue[T any](maxChunk int) *ChunkQueue[T] {
	q := &ChunkQueue[T]{data: make([]T, 0), maxChunk: maxChunk}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// ErrQueueClosed is returned when adding to a closed queue.
var ErrQueueClosed = errors.New("queue is closed")

// Add adds a single item to the queue.
func (q *ChunkQueue[T]) Add(item T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.isClosed {
		return ErrQueueClosed
	}

	q.data = append(q.data, item)
	q.cond.Signal()
	return nil
}

// AddBatch adds multiple items to the queue in one operation.
func (q *ChunkQueue[T]) AddBatch(items []T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.isClosed {
		return ErrQueueClosed
	}

	q.data = append(q.data, items...)
	q.cond.Broadcast()
	return nil
}

// ReadChunk reads a chunk of up to maxChunk items from the queue.
func (q *ChunkQueue[T]) ReadChunk() ([]T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.data) == 0 && !q.isClosed {
		q.cond.Wait()
	}

	if len(q.data) == 0 && q.isClosed {
		return nil, false
	}

	n := q.maxChunk
	if len(q.data) < n {
		n = len(q.data)
	}

	chunk := q.data[:n]
	q.data = q.data[n:]

	return chunk, true
}

// Close marks the queue as closed.
func (q *ChunkQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.isClosed = true
	q.cond.Broadcast()
}
