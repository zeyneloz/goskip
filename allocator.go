package goskip

import (
	"sync/atomic"
	"unsafe"
)

/*
 Don't forget about cpu cache model. Write code that works with it not works against it!
 Optimize for x64.

 ==Data Padding:
 Problem:
 You have a data structure that does not fit nicely into the
 cache line of the machine. When you parallelize your code,
 your performance suffers because of false sharing. How do
 you transform your code to reduce false sharing and improve
 overall performance?

 Solution:
 Pad your data structure with extraneous “padding” data.
 Padding data do not contribute to the functionality of your
 program; they only expand the size of your data structure
 so that it fits into an entire cache line In effect, this grants
 your data exclusivity to the entire cache line.


 ==Data Alignment:
 Problem:
 You have a data structure that could fit within a cache
 line but has been allocated such that it crosses the boundary
 of a cache line incurring an additional overhead.

 All primitive data types (int, char, float, double) have a
 natural alignment within memory

 Solution:
 Align your data structure so that it starts on a cache line
 boundary. Similar to Data Padding, you should set the
 alignment to the size of the cache line.
*/

// Default size of the node type, in bytes.
// This value will be used for allocating memory for node.
const defaultNodeSize = uint32(unsafe.Sizeof(node{}))

// 0 means nil pointer for offsets of Allocator.
const nilAllocatorOffset = uint32(0)
const initialAllocatorOffset = uint32(1)

// const cacheLineSize = 64
// const paddingLimit = 15

type Allocator struct {
	// Actual memory space where we keep the data.
	mem []byte

	// Pointer to the beginning of available memory.
	// Max addressable memory 2^32 = 4GB.
	offset uint32
}

// NewAllocator allocates a buffer with given size and returns a new allocator.
func NewAllocator(size uint32) *Allocator {
	// Set initial offset as 1 since 0 is used for nil pointers.
	return &Allocator{make([]byte, size), initialAllocatorOffset}
}

// New allocates memory.
// Offset of the space in the mem is returned.
func (allc *Allocator) New(size uint32) uint32 {
	// Multiple goroutines might modify offset value.
	// We need to calculate new offset atomically.
	// TODO overflow
	newOffset := atomic.AddUint32(&allc.offset, size)
	return newOffset - size
}

// PutBytes will copy given value into mem and returns offset.
func (allc *Allocator) PutBytes(val []byte) uint32 {
	valSize := uint32(len(val))
	// Add padding for increasing cache performance.
	/*padding := valSize % cacheLineSize
	if padding < paddingLimit {
		valSize += padding
	}*/
	offset := allc.New(valSize)
	copy(allc.mem[offset:], val)
	return offset
}

// PutBytesTo will copy given value into given memory offset.
func (allc *Allocator) PutBytesTo(offset uint32, val []byte) {
	copy(allc.mem[offset:], val)
}

// MakeNode will allocate required space for node type.
// The offset of the node in the mem is returned.
func (allc *Allocator) MakeNode(truncatedSize uint32) uint32 {
	// Calculate the amount of actual memory required for this node.
	// Depending on the height of the node, size might be truncated.
	size := defaultNodeSize - truncatedSize
	/*padding := size % cacheLineSize
	if padding < paddingLimit {
		size += padding
	}*/
	return allc.New(size)
}

// GetBytes returns byte slice at offset with given size.
func (allc *Allocator) GetBytes(offset uint32, size uint32) []byte {
	return allc.mem[offset : offset+size]
}

// GetNode returns a pointer to the node at offset.
func (allc *Allocator) GetNode(offset uint32) *node {
	if offset == nilAllocatorOffset {
		return nil
	}
	return (*node)(unsafe.Pointer(&allc.mem[offset]))
}

func (allc *Allocator) getOffset() uint32 {
	return atomic.LoadUint32(&allc.offset)
}
