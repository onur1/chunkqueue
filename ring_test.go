package chunkqueue_test

import (
	"math"
	"testing"

	"github.com/onur1/chunkqueue"
	"github.com/stretchr/testify/assert"
)

func TestRing(t *testing.T) {
	testCases := []struct {
		desc string
		f    func(t *testing.T)
	}{
		{
			desc: "put and get",
			f: func(t *testing.T) {
				list := chunkqueue.NewRing[int](2)

				list.Put(0, 42)
				list.Put(1, 555)

				assert.EqualValues(t, 42, list.Get(0))
				assert.EqualValues(t, 555, list.Get(1))
			},
		},
		{
			desc: "overflow",
			f: func(t *testing.T) {
				list := chunkqueue.NewRing[int](2)

				list.Put(0, 42)
				list.Put(1, 555)
				list.Put(2, 30)

				assert.EqualValues(t, 30, list.Get(0))
				assert.EqualValues(t, 555, list.Get(1))
				assert.EqualValues(t, 30, list.Get(2))

				assert.EqualValues(t, 555, list.Get(math.MaxInt))
				n := 1
				assert.EqualValues(t, 30, list.Get(math.MaxInt+n))
				n = 2
				list.Put(math.MaxInt+n, 83)
				assert.EqualValues(t, 83, list.Get(math.MaxInt+n))
			},
		},
		{
			desc: "negative indice",
			f: func(t *testing.T) {
				list := chunkqueue.NewRing[int](2)

				list.Put(0, 42)
				list.Put(1, 555)
				list.Put(2, 30)

				assert.EqualValues(t, 30, list.Get(0))
				assert.EqualValues(t, 555, list.Get(-1))
				assert.EqualValues(t, 30, list.Get(-2))
				assert.EqualValues(t, 555, list.Get(-3))
			},
		},
		{
			desc: "del",
			f: func(t *testing.T) {
				list := chunkqueue.NewRing[int](2)

				list.Put(0, 42)
				assert.EqualValues(t, 42, list.Get(0))

				list.Del(0)
				assert.EqualValues(t, 0, list.Get(0))
			},
		},
		{
			desc: "multiple of two",
			f: func(t *testing.T) {
				list := chunkqueue.NewRing[int](3)

				assert.EqualValues(t, 4, list.Size())
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.f)
	}
}
