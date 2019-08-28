package goskip

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// default allocator size for both main and value allocator.
// 128 KB
const defaultAllocatorSize = uint32(2 << 17)

// An empty long value to be used in tests.
var longValue = [2 << 10]byte{}

// Sample node data for tests.
// Assumptions:
//  - length of the values are greater than 1.
//  - keys are unique among nodes.
//  = height can not exceed DefaultMaxHeight.
var nodesData = []struct {
	key    []byte
	val    []byte
	height uint8
}{
	{[]byte("key1"), []byte("value1"), uint8(4)},
	{[]byte("blue key"), []byte("red value"), uint8(1)},
	{[]byte("when"), []byte("10082016"), uint8(13)},
	{[]byte("tes.key.new"), []byte("kksmdns77aywemjjwncwj222"), uint8(11)},
	{[]byte("less.key.a"), []byte("kksls"), uint8(7)},
	{[]byte("almost_empty_key"), []byte("  "), uint8(4)},
	{[]byte("long_key"), longValue[:], uint8(11)},
	{[]byte("appa"), []byte("momo"), uint8(3)},
	{[]byte("ffffff"), []byte("#aaaaaaa"), uint8(3)},
	{[]byte("1key"), []byte("11"), uint8(16)},
	{[]byte("5key"), []byte("55"), uint8(7)},
	{[]byte("3key"), []byte("33"), uint8(4)},
}

// Return value of given node encodedValue in bytes.
func getNodeValue(allc *Allocator, encodedValue uint64) []byte {
	val := atomic.LoadUint64(&encodedValue)
	return allc.getBytes(uint32(val>>32), uint32(val))
}

// Return key of a node in bytes, using offset and size.
func getNodeKey(allc *Allocator, offset uint32, size uint16) []byte {
	return allc.getBytes(offset, uint32(size))
}

// createAllocators creates two allocator and returns them.
func createAllocators(mainSize uint32, valSize uint32) (*Allocator, *Allocator) {
	mainAllocator := newAllocator(mainSize)
	valueAllocator := newAllocator(valSize)
	return mainAllocator, valueAllocator
}

func TestNewNode(t *testing.T) {
	keyAllc, valAllc := createAllocators(defaultAllocatorSize, defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, _ := newNode(keyAllc, valAllc, data.height, data.key, data.val)
			assert.Equal(t, data.height, node.height, "Height must be initialized correctly.")
			assert.Equal(t, data.key, getNodeKey(keyAllc, node.keyOffset, node.keySize),
				"Key must be initialized correctly.")
			assert.Equal(t, data.val, getNodeValue(valAllc, node.encodedValue),
				"Height must be initialized correctly.")
		})
	}
}

func TestNewNode_Parallel(t *testing.T) {
	keyAllc, valAllc := createAllocators(defaultAllocatorSize, defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, _ := newNode(keyAllc, valAllc, data.height, data.key, data.val)
			assert.Equal(t, data.height, node.height, "Height must be initialized correctly.")
			assert.Equal(t, data.key, getNodeKey(keyAllc, node.keyOffset, node.keySize),
				"Key must be initialized correctly.")
			assert.Equal(t, data.val, getNodeValue(valAllc, node.encodedValue),
				"Height must be initialized correctly.")
		})
	}
}

func TestNode_GetNextNodeOffset(t *testing.T) {
	keyAllc, valAllc := createAllocators(defaultAllocatorSize, defaultAllocatorSize)
	node, _ := newNode(keyAllc, valAllc, nodesData[0].height, nodesData[0].key, nodesData[0].val)
	node.layers[0] = 3
	node.layers[1] = 65
	node.layers[5] = 4441
	assert.Equal(t, uint32(3), node.getNextNodeOffset(0))
	assert.Equal(t, uint32(65), node.getNextNodeOffset(1))
	assert.Equal(t, uint32(4441), node.getNextNodeOffset(5))
}

func TestNode_EncodeValue(t *testing.T) {
	keyAllc, valAllc := createAllocators(defaultAllocatorSize, defaultAllocatorSize)
	node, _ := newNode(keyAllc, valAllc, nodesData[0].height, nodesData[0].key, nodesData[0].val)
	offset := uint32(2 << 7)
	size := uint32(2<<12) + 1
	node.encodeValue(offset, size)
	val := atomic.LoadUint64(&node.encodedValue)
	assert.Equal(t, offset, uint32(val>>32))
	assert.Equal(t, size, uint32(val))
}

func TestNode_DecodeValue(t *testing.T) {
	keyAllc, valAllc := createAllocators(defaultAllocatorSize, defaultAllocatorSize)
	node, _ := newNode(keyAllc, valAllc, nodesData[0].height, nodesData[0].key, nodesData[0].val)
	offset := uint32(2 << 7)
	size := uint32(2<<12) + 1
	node.encodeValue(offset, size)
	parsedOffset, parsedSize := node.decodeValue()
	assert.Equal(t, offset, parsedOffset)
	assert.Equal(t, size, parsedSize)
}

func TestNewSkipList(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	assert.Equal(t, uint32(0), s.height, "Skip List height must be 0 initially")
	assert.NotNil(t, s.mainAllocator, "Skip List must have main allocator")
	assert.NotNil(t, s.valueAllocator, "Skip List must have value allocator")
	assert.NotNil(t, s.head, "Skip List must have initial node")
}

func TestSkipList_GetNode(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, offset := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, node, s.getNode(offset))
		})
	}
}

func TestSkipList_GetNode_Parallel(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, offset := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, node, s.getNode(offset))
		})
	}
}

func TestSkipList_GetNodeKey(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, data.key, s.getNodeKey(node))
		})
	}
}

func TestSkipList_GetNodeKey_Parallel(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, data.key, s.getNodeKey(node))
		})
	}
}

func TestSkipList_GetNodeValue(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, data.val, s.getNodeValue(node))
		})
	}
}

func TestSkipList_GetNodeValue_Parallel(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			assert.Equal(t, data.val, s.getNodeValue(node))
		})
	}
}

func TestSkipList_SetNodeValue(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	// Run for the case that length of new value is less than length of old value.
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			newVal := data.val[1:]
			s.setNodeValue(node, newVal)
			assert.Equal(t, newVal, s.getNodeValue(node))
		})
	}
	// Run for the case that length of new value is greater than length of old value.
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			newVal := append([]byte("new-"), data.val...)
			s.setNodeValue(node, newVal)
			assert.Equal(t, newVal, s.getNodeValue(node))
		})
	}
}

func TestSkipList_SetNodeValue_Parallel(t *testing.T) {
	s := NewSkipList(defaultAllocatorSize)
	// Run for the case that length of new value is less than length of old value.
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			newVal := data.val[1:]
			s.setNodeValue(node, newVal)
			assert.Equal(t, newVal, s.getNodeValue(node))
		})
	}
	// Run for the case that length of new value is greater than length of old value.
	for i, data := range nodesData {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			t.Parallel()
			node, _ := newNode(s.mainAllocator, s.valueAllocator, data.height, data.key, data.val)
			newVal := append([]byte("new-"), data.val...)
			s.setNodeValue(node, newVal)
			assert.Equal(t, newVal, s.getNodeValue(node))
		})
	}
}
