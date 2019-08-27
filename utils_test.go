package goskip

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareKeys(t *testing.T) {
	key1 := []byte("aaaa")
	key2 := []byte("aaab")
	key3 := []byte("a")
	assert.Equal(t, compareKeys(key1, key1), 0, "Must return 0")
	assert.Equal(t, compareKeys(key2, key1), 1, "Must return 1")
	assert.Equal(t, compareKeys(key3, key1), -1, "Must return -1")
}
