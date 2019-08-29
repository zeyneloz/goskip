package goskip

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	DefaultMaxHeight = 24
	LayerSize        = int(unsafe.Sizeof(uint32(0)))
	defaultLevelP    = 0.5
)

// A note on CPU Cache Performance:
// Try to align your structures with cache line size.
// For structures that generally contain data elements of different types,
// the compiler tries to maintain proper alignment of data elements by
// inserting unused memory between elements. This technique is known as
// 'Padding'. Also, the compiler aligns the entire structure to its most
// strictly aligned member. The compiler may also increase the size of
// structure, if necessary, to make it a multiple of  the alignment
// by adding padding at the end of the structure.
// This is known as 'Tail Padding'.

// Since the size of the listNode is bigger than usual cache line
// size (64 Bytes on most CPU), it is beneficial to keep
// the parts of the structure that are touched together in memory,
// which may lead to improved cache locality.
type node struct {
	// valSize (uint32) and valOffset (uint32) is encoded as a single uint64 value,
	// so that we can atomically load this values.
	// valSize -> bits 0-31
	// valOffset -> bits 32-63
	encodedValue uint64

	// Height of the current node.
	// 1 < height < maxHeight.
	// Height never changes even on
	height uint8 // 1 Byte

	// Key offset and size in allocator.
	// keyOffset and keySize never changes
	keyOffset uint32 // 4 Bytes
	keySize   uint16 // 2 Bytes

	// Instead of creating a new node for the same key,
	// use existing node to save space (also for improved cache locality).
	// layers[0] represents the first level (which has height 1).
	// layers definition should always be at the end of the struct since
	// we might allocate less space for it in memory to reduce
	// memory footprint.
	layers [DefaultMaxHeight]uint32 // 4 Byte for each level. Average: 32 Byte
}

// SkipList represents a skip list.
type SkipList struct {
	// Current height of the list.
	height uint32

	// Head node.
	head *node

	// Value Allocator is used for allocating node values.
	valueAllocator *Allocator

	// Root Allocator is used for every other allocation: key, node etc...
	mainAllocator *Allocator
}

// newNode creates a node with given height and returns node and the offset.
func newNode(allc *Allocator, valAllc *Allocator, height uint8, key []byte, val []byte) (*node, uint32) {
	truncatedSize := (DefaultMaxHeight - int(height)) * LayerSize
	keyOffset := allc.putBytes(key)
	nodeOffset := allc.makeNode(uint32(truncatedSize))
	valOffset := valAllc.putBytes(val)
	node := allc.getNode(nodeOffset)
	node.height = height
	node.keyOffset = keyOffset
	// TODO key length should not exceed uint16 size, assert.
	node.keySize = uint16(len(key))
	node.encodeValue(valOffset, uint32(len(val)))
	return node, nodeOffset
}

// Returns the offset of next node on given level (height).
func (n *node) getNextNodeOffset(level uint8) uint32 {
	// Layers can be altered concurrently. Use atomic load.
	return atomic.LoadUint32(&n.layers[level])
}

// casNextNodeOffset sets the offset of next node on a level, using cas operation.
func (n *node) casNextNodeOffset(level uint8, old uint32, new uint32) bool {
	return atomic.CompareAndSwapUint32(&n.layers[level], old, new)
}

// Set value offset and size.
func (n *node) encodeValue(offset uint32, size uint32) {
	encodedValue := uint64(size)
	encodedValue += uint64(offset) << 32
	atomic.StoreUint64(&n.encodedValue, encodedValue)
}

// Returns (offset, size) of value.
func (n *node) decodeValue() (uint32, uint32) {
	val := atomic.LoadUint64(&n.encodedValue)
	return uint32(val >> 32), uint32(val)
}

// Returns a pointer to node with given offset.
func (s *SkipList) getNodeKey(node *node) []byte {
	return s.mainAllocator.getBytes(node.keyOffset, uint32(node.keySize))
}

// Returns a pointer to node with given offset.
func (s *SkipList) getNodeValue(node *node) []byte {
	offset, size := node.decodeValue()
	return s.valueAllocator.getBytes(offset, size)
}

// Returns a pointer to node with given offset.
// Returns nil if given offset is nilAllocatorOffset.
func (s *SkipList) getNode(offset uint32) *node {
	return s.mainAllocator.getNode(offset)
}

// Set the value of given node.
func (s *SkipList) setNodeValue(node *node, val []byte) {
	newValSize := uint32(len(val))
	valOffset, valSize := node.decodeValue()
	// If node currently has a value and the size of the value is bigger than new value,
	// use previous value's memory for new value.
	if valSize >= newValSize {
		s.valueAllocator.putBytesTo(valOffset, val)
		node.encodeValue(valOffset, newValSize)
		return
	}
	// If the length of new node is greater than odl node, forget old value
	// and allocate new space in memory for new value.
	newOffset := s.valueAllocator.putBytes(val)
	node.encodeValue(newOffset, newValSize)
}

// getNeighbourNodes returns nodes (x, y, z) where
// x is the right most node asserts x.key <= key.
// y is the offset of next node of x, on this level.
// z is true whenever x.key = key
// startingNode is used as starting point for search (hint from previous calls.)
// Key of the startingNode always must be less then given key.
func (s *SkipList) getNeighbourNodes(startingNode *node, level uint8, key []byte) (*node, uint32, bool) {
	currentNode := startingNode
	for {

		nextNodeOffset := currentNode.getNextNodeOffset(level)
		nextNode := s.getNode(nextNodeOffset)
		if nextNode == nil {
			return currentNode, nextNodeOffset, false
		}

		nextNodeKey := s.getNodeKey(nextNode)
		cmp := compareKeys(nextNodeKey, key)

		if cmp == 0 {
			return nextNode, nextNodeOffset, true
		}
		if cmp > 0 {
			return currentNode, nextNodeOffset, false
		}

		// Go to next node in this level.
		currentNode = nextNode
	}
}

// getClosestNode returns the closest node whose key <= given key,
// along with a boolean value which designates whether returned nodes key
// is equal to given key.
func (s *SkipList) getClosestNode(key []byte) (*node, bool) {
	currentNode := s.head        // points to current node in loop.
	level := uint8(s.height - 1) // current level
	for {
		nextNodeOffset := currentNode.getNextNodeOffset(level)
		nextNode := s.getNode(nextNodeOffset)
		// if there is no next node on this level.
		if nextNode == nil {
			// If there are still levels to descend to, go one level lower.
			// Else, return current node as closest node.
			if level > 0 {
				level--
				continue // used here to get rid of nested if/else.
			}
			return currentNode, false
		}

		nextNodeKey := s.getNodeKey(nextNode)
		cmp := compareKeys(nextNodeKey, key)

		// If the node is found, return it.
		if cmp == 0 {
			return nextNode, true
		}

		// If next node's key is less than the key we search for,
		// set currentNode as next node and continue searching.
		// Go right in skip list.
		if cmp < 0 {
			currentNode = nextNode
			continue
		}

		// If next node's key is greater than the key we search for,
		// a) Go one level down in skip list if possible.
		if level > 0 {
			level--
			continue // used here to get rid of nested if/else.
		}
		// b) Return current node as closest node if this is the base level.
		return currentNode, false
	}
}

// Get returns value for given key if it exists,
// returns nil otherwise.
func (s *SkipList) Get(key []byte) []byte {
	node, found := s.getClosestNode(key)
	if !found {
		return nil
	}
	return s.getNodeValue(node)
}

// Set inserts given key-value pair into list.
func (s *SkipList) Set(key []byte, val []byte) {
	listHeight := s.getHeight()

	var prevNodes [DefaultMaxHeight + 1]*node
	var nextNodesOffsets [DefaultMaxHeight + 1]uint32
	var sameKey bool

	prevNodes[listHeight] = s.head

	// Starting from the highest level, find the suitable position to
	// put the node for each level.
	for i := int(listHeight) - 1; i >= 0; i-- {
		prevNodes[i], nextNodesOffsets[i], sameKey = s.getNeighbourNodes(prevNodes[i+1], uint8(i), key)
		// if there is already a node with the same key, there is no need to
		// create a new node, just use it.
		if sameKey {
			s.setNodeValue(prevNodes[i], val)
			return
		}
	}

	// Create a new node.
	nodeHeight := s.randomHeight()
	node, nodeOffset := newNode(s.mainAllocator, s.valueAllocator, nodeHeight, key, val)

	// If the height of new node is more then current height of the list,
	// try to increase list height using CAS, since it can be changed.
	for listHeight < uint32(nodeHeight) {
		if s.casHeight(listHeight, uint32(nodeHeight)) {
			break
		}
		listHeight = s.getHeight()
	}

	// Start from base level and put new node.
	// We are starting from base level to prevent race conditions
	// for neighbors.
	for i := uint8(0); i < nodeHeight; i++ {
		for {
			// if prevNodes[i] is nil, it means at the time of search,
			// list height was less than new node length.
			// We need to discover this level.
			if prevNodes[i] == nil {
				prevNodes[i], nextNodesOffsets[i], _ = s.getNeighbourNodes(s.head, i, key)
			}

			node.layers[i] = nextNodesOffsets[i]
			if prevNodes[i].casNextNodeOffset(i, nextNodesOffsets[i], nodeOffset) {
				break
			}
			// If cas fails, we need to rediscover this level
			prevNodes[i], nextNodesOffsets[i], sameKey = s.getNeighbourNodes(prevNodes[i], i, key)
			if sameKey {
				s.setNodeValue(prevNodes[i], val)
				return
			}
		}
	}

}

// casHeight performs cas operation on list height.
func (s *SkipList) casHeight(old uint32, new uint32) bool {
	return atomic.CompareAndSwapUint32(&s.height, old, new)
}

// getHeight returns the maximum height of its nodes.
func (s *SkipList) getHeight() uint32 {
	// Height can be modified concurrently, so we need to load it atomically.
	return atomic.LoadUint32(&s.height)
}

// randomHeight returns a random number between 1 and maxHeight of the list.
func (s *SkipList) randomHeight() uint8 {
	height := 1
	// TODO is there a more performant way to produce random level?
	for height < DefaultMaxHeight && rand.Float64() < defaultLevelP {
		height++
	}
	return uint8(height)
}

// NewSkipList initializes and returns a skip list instance.
func NewSkipList(allocatorSize uint32) *SkipList {
	mainAllocator := newAllocator(allocatorSize)
	valueAllocator := newAllocator(allocatorSize)
	var emptyValue []byte
	head, _ := newNode(mainAllocator, valueAllocator, DefaultMaxHeight, emptyValue, emptyValue)
	return &SkipList{
		mainAllocator:  mainAllocator,
		valueAllocator: valueAllocator,
		height:         0,
		head:           head,
	}
}
