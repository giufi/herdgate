package inmemorystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore_NewIsolated(t *testing.T) {
	key := "key"
	want := []byte("foo")

	s1 := New()
	s2 := New()
	s1.Set(key, want)

	got, ok := s1.Get(key)
	assert.True(t, ok)
	assert.Equal(t, want, got)

	got, ok = s2.Get(key)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestStore_GetMiss(t *testing.T) {
	key := "key"
	otherKey := "otherKey"

	s := New()
	s.Set(otherKey, []byte("foo"))

	got, ok := s.Get(key)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestStore_GetHit(t *testing.T) {
	testCases := map[string][]byte{
		"key1": []byte("foo"),
		"key2": []byte("bar"),
		"key3": nil,
	}

	s := New()
	for key, value := range testCases {
		s.Set(key, value)
	}

	for key, want := range testCases {
		t.Run(key, func(t *testing.T) {
			got, ok := s.Get(key)
			assert.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestStore_SetThenOverwrite(t *testing.T) {
	key := "key"
	original := []byte("foo")
	want := []byte("bar")

	s := New()
	s.Set(key, original)

	got, ok := s.Get(key)
	assert.True(t, ok)
	assert.Equal(t, original, got)

	s.Set(key, want)

	got, ok = s.Get(key)
	assert.True(t, ok)
	assert.Equal(t, want, got)
}
