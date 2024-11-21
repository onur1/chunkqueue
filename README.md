# chunkqueue

`ChunkQueue` is a thread-safe, generic queue with support for chunk-based reading.

## Installation

```bash
go get github.com/onur1/chunkqueue
```

## Usage

### Basic Example
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/onur1/chunkqueue"
)

func main() {
	// Create a new ChunkQueue
	q := chunkqueue.NewChunkQueue[int]()

	// Producer: Add items to the queue
	go func() {
		for i := 1; i <= 10; i++ {
			_ = q.Push(context.Background(), i)
		}
		q.Close() // Signal that no more items will be added
	}()

	// Consumer: Read chunks from the queue
	for {
		chunk, err := q.Pop(context.Background(), 3)
		if err == chunkqueue.ErrQueueClosed {
			break // Exit when the queue is closed and empty
		}
		fmt.Println("Read chunk:", chunk)
	}
}

// Output:
// Read chunk: [1 2 3]
// Read chunk: [4 5 6]
// Read chunk: [7 8 9]
// Read chunk: [10]
```

### Context Cancellation Example
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/onur1/chunkqueue"
)

func main() {
	// Create a new ChunkQueue
	q := chunkqueue.NewChunkQueue[int]()

	// Producer: Add items to the queue
	go func() {
		for i := 1; i <= 10; i++ {
			_ = q.Push(context.Background(), i)
			time.Sleep(50 * time.Millisecond) // Simulate production delay
		}
		q.Close()
	}()

	// Consumer with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	for {
		time.Sleep(100 * time.Millisecond) // Simulate consumer delay

		chunk, err := q.Pop(ctx, 3)
		if err == context.DeadlineExceeded {
			fmt.Println("Operation timed out")
			break
		}
		if err == chunkqueue.ErrQueueClosed {
			break
		}
		fmt.Println("Read chunk:", chunk)
	}
}

// Output:
// Read chunk: [1 2]
// Read chunk: [3 4]
// Read chunk: [5 6]
// Read chunk: [7 8 9]
// Operation timed out
```

## API

### `NewChunkQueue`

```go
func NewChunkQueue[T any]() *ChunkQueue[T]
```

Creates a new `ChunkQueue` instance.

### `Push`

```go
func (q *ChunkQueue[T]) Push(ctx context.Context, item ...T) error
```

Adds one or more items to the queue. Blocks if the context is canceled. Returns:
- `context.Canceled` if the context is canceled before the operation completes.
- `ErrQueueClosed` if the queue is closed.

### `Pop`

```go
func (q *ChunkQueue[T]) Pop(ctx context.Context, size int) ([]T, error)
```

Removes and returns up to `size` items from the queue. Blocks until items are available or the queue is empty and closed. Returns:
- `context.Canceled` if the context is canceled before the operation completes.
- `ErrQueueClosed` when the queue is closed and fully drained.

### `Close`

```go
func (q *ChunkQueue[T]) Close()
```

Marks the queue as closed. Signals all waiting goroutines that no more items will be added.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
