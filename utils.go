package goskip

import "bytes"

// compareKeys returns an integer comparing two keys lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// A nil argument is equivalent to an empty slice.
func compareKeys(keyA []byte, keyB []byte) int {
	return bytes.Compare(keyA, keyB)
}