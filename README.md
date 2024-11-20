# chunkqueue

`ChunkQueue` provides a thread-safe queue with chunk-based reading and support for both single and batch inserts.

## Features

- **Chunked Reading**: Read items in configurable chunk sizes.
- **Single and Batch Inserts**: Add items one at a time or in batches for flexibility.
- **Thread-safe**: Multiple producers and consumers can safely access the queue concurrently.

## Installation

```bash
go get github.com/onur1/chunkqueue
```

## Usage

### Basic Example
```go
package main

import (
	"fmt"

	"github.com/onur1/chunkqueue"
)

func main() {
	// Create a ChunkQueue with a maximum chunk size of 4
	chunkQueue := chunkqueue.NewChunkQueue[int](4)

	// Producer: Add items to the queue
	go func() {
		for i := 1; i <= 10; i++ {
			chunkQueue.Add(i)
		}
		chunkQueue.Close() // Signal that no more items will be added
	}()

	// Consumer: Read chunks from the queue
	for {
		chunk, ok := chunkQueue.ReadChunk()
		if !ok {
			break // Exit when the queue is closed and empty
		}
		fmt.Println("Read chunk:", chunk)
	}
}

// Output:
// Read chunk: [1 2 3 4]
// Read chunk: [5 6 7 8]
// Read chunk: [9 10]
```

### Batch Insert Example
```go
chunkQueue := NewChunkQueue

// Add a batch of items to the queue
chunkQueue.AddBatch([]int{1, 2, 3, 4, 5})
chunkQueue.AddBatch([]int{6, 7, 8, 9, 10})

// Consume the chunks
for {
	chunk, ok := chunkQueue.ReadChunk()
	if !ok {
		break
	}
	fmt.Println("Read chunk:", chunk)
}
```

## API

### `NewChunkQueue`

```go
NewChunkQueue[T any](maxChunk int) *ChunkQueue[T]
```

Creates a new `ChunkQueue` instance with the specified maximum chunk size.

### `Add`

```go
Add(item T)
```

Adds a single item to the queue.

### `AddBatch`

```go
AddBatch(items []T)
```

Adds multiple items to the queue in one operation.

### `ReadChunk`

```go
ReadChunk() ([]T, bool)
```

Reads a chunk of up to `maxChunk` items from the queue. Blocks until items are available or the queue is closed. Returns the chunk and a boolean indicating success.

### `Close`

```go
Close()
```

Marks the queue as closed. Signals all waiting goroutines that no more items will be added.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
