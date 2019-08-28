package goskip

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Default allocator size to be used in tests. 64 KB
const allocatorSize = uint32(2 << 16)

func TestNewAllocator(t *testing.T) {
	a := newAllocator(allocatorSize)
	assert.Equal(t, allocatorSize, uint32(len(a.mem)), "Allocator memory size must be equal to given size")
	assert.Equal(t, initialAllocatorOffset, a.offset, "Allocator offset must be 1 offset after init")
}

func TestAllocator_New(t *testing.T) {
	a := newAllocator(allocatorSize)
	valSize := uint32(304)
	lastOffset := initialAllocatorOffset
	for i := 0; i < 10; i++ {
		offset := a.new(valSize)
		assert.Equal(t, lastOffset, offset, "Offset must be set correctly after calling New")
		lastOffset = offset + valSize
	}
}

func TestAllocator_New_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	valSize := uint32(11)
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			offsetBefore := a.getOffset()
			offset := a.new(valSize)
			assert.GreaterOrEqual(t, offset, offsetBefore, "Offset must be greater or equal - parallel")
		})
	}
}

func TestAllocator_PutBytes(t *testing.T) {
	a := newAllocator(allocatorSize)
	val := []byte("such_a_small_value")
	valSize := uint32(len(val))
	lastOffset := initialAllocatorOffset
	for i := 0; i < 20; i++ {
		offset := a.putBytes(val)
		assert.Equal(t, lastOffset, offset, "Offset must be set correctly after calling putBytes")
		lastOffset = offset + valSize
	}
}

func TestAllocator_PutBytes_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	val := []byte("such_a_small_value")
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			offsetBefore := a.getOffset()
			offset := a.putBytes(val)
			assert.GreaterOrEqual(t, offset, offsetBefore, "Offset must be greater or equal - parallel")
		})
	}
}

func TestAllocator_PutBytesTo(t *testing.T) {
	a := newAllocator(allocatorSize)
	val := []byte("such_a_small_value_3")
	valSize := uint32(len(val))
	offset := uint32(2 << 2)
	for i := 0; i < 20; i++ {
		a.putBytesTo(offset, val)
		offset += valSize
	}

	offset = uint32(2 << 2)
	for i := 0; i < 20; i++ {
		assert.Equal(t, val, a.mem[offset:offset+valSize])
		offset += valSize
	}
}

func TestAllocator_PutBytesTo_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	val := []byte("such_a_small_value_3")
	valSize := uint32(len(val))
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			offset := valSize * uint32(i)
			a.putBytesTo(offset, val)
			assert.Equal(t, val, a.mem[offset:offset+valSize])
		})
	}
}

func TestAllocator_MakeNode(t *testing.T) {
	a := newAllocator(allocatorSize)
	t.Run("FullNodeSize", func(t *testing.T) {
		offset := a.makeNode(0)
		assert.Equal(t, a.getOffset(), offset+defaultNodeSize, "New offset must be old + node size")
	})
	t.Run("TruncatedNodeSize", func(t *testing.T) {
		truncatedSize := uint32(96)
		offset := a.makeNode(truncatedSize)
		assert.Equal(t, a.getOffset(), offset+defaultNodeSize-truncatedSize, "New offset must be old + node size(truncated)")
	})
	t.Run("ValidNode", func(t *testing.T) {
		offset := a.makeNode(0)
		node := (*node)(unsafe.Pointer(&a.mem[offset]))
		assert.Equal(t, uint8(0), node.height, "Smoke test for allocated node")
	})
}

func TestAllocator_MakeNode_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			offset := a.makeNode(0)
			node := (*node)(unsafe.Pointer(&a.mem[offset]))
			assert.Equal(t, uint8(0), node.height, "Smoke test for node - parallel")
		})
	}
}

func TestAllocator_GetBytes(t *testing.T) {
	a := newAllocator(allocatorSize)
	valString := "sample key %d"
	for i := 0; i < 400; i++ {
		val := []byte(fmt.Sprintf(valString, i))
		valSize := uint32(len(val))
		offset := a.putBytes(val)
		assert.Equal(t, val, a.getBytes(offset, valSize), "GetBytes must return correct value")
	}
}

func TestAllocator_GetBytes_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	valString := "sample key %d"
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			val := []byte(fmt.Sprintf(valString, i))
			valSize := uint32(len(val))
			offset := a.putBytes(val)
			assert.Equal(t, val, a.getBytes(offset, valSize), "GetBytes must return correct value - parallel")
		})
	}
}

func TestAllocator_GetNode(t *testing.T) {
	a := newAllocator(allocatorSize)
	for i := 0; i < 400; i++ {
		offset := a.makeNode(0)
		node := a.getNode(offset)
		assert.Equal(t, uint8(0), node.height, "Smoke test for GetNode")
	}
}

func TestAllocator_GetNode_Parallel(t *testing.T) {
	a := newAllocator(allocatorSize)
	for i := 0; i < 400; i++ {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			offset := a.makeNode(0)
			node := a.getNode(offset)
			assert.Equal(t, uint8(0), node.height, "Smoke test for GetNode - parallel")
		})
	}
}
