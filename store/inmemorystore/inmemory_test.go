package inmemorystore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	_key   = "key"
	_noTTL = 0
)

func TestStore_NewIsolated(t *testing.T) {
	want := []byte("foo")

	s1 := New()
	s2 := New()
	s1.Set(t.Context(), _key, want, _noTTL)

	got, ok := s1.Get(t.Context(), _key)
	assert.True(t, ok)
	assert.Equal(t, want, got)

	got, ok = s2.Get(t.Context(), _key)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestStore_NewUTC(t *testing.T) {
	assert.Equal(t, time.UTC, New().now().Location())
}

func TestStore_GetMiss(t *testing.T) {
	otherKey := "otherKey"

	s := New()
	s.Set(t.Context(), otherKey, []byte("foo"), _noTTL)

	got, ok := s.Get(t.Context(), _key)
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
	for k, v := range testCases {
		s.Set(t.Context(), k, v, _noTTL)
	}

	for k, want := range testCases {
		t.Run(k, func(t *testing.T) {
			got, ok := s.Get(t.Context(), k)
			assert.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestStore_GetTTL(t *testing.T) {
	setAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := []byte("foo")

	testCases := map[string]struct {
		ttl     time.Duration
		delay   time.Duration
		wantHit bool
	}{
		"not yet expired": {ttl: time.Minute, delay: 59 * time.Second, wantHit: true},
		"exactly at ttl":  {ttl: time.Minute, delay: time.Minute, wantHit: true},
		"expired":         {ttl: time.Minute, delay: time.Minute + time.Second, wantHit: false},
		"never expires":   {ttl: _noTTL, delay: 24 * time.Hour, wantHit: true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			now := setAt
			s := New()
			s.now = func() time.Time { return now }

			s.Set(t.Context(), _key, want, tc.ttl)
			now = setAt.Add(tc.delay)

			got, ok := s.Get(t.Context(), _key)
			assert.Equal(t, tc.wantHit, ok)
			if tc.wantHit {
				assert.Equal(t, want, got)
				return
			}
			assert.Nil(t, got)
			_, exists := s.entries[_key]
			assert.False(t, exists)
		})
	}
}

func TestStore_SetValueOverwrite(t *testing.T) {
	original := []byte("foo")
	want := []byte("bar")

	s := New()
	s.Set(t.Context(), _key, original, _noTTL)

	got, ok := s.Get(t.Context(), _key)
	assert.True(t, ok)
	assert.Equal(t, original, got)

	s.Set(t.Context(), _key, want, _noTTL)

	got, ok = s.Get(t.Context(), _key)
	assert.True(t, ok)
	assert.Equal(t, want, got)
}

func TestStore_SetTTL(t *testing.T) {
	setAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := []byte("foo")

	testCases := map[string]struct {
		ttl           time.Duration
		wantExpiresAt time.Time
	}{
		"positive ttl": {ttl: time.Minute, wantExpiresAt: setAt.Add(time.Minute)},
		"zero ttl":     {ttl: _noTTL},
		"negative ttl": {ttl: -time.Minute},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			s := New()
			s.now = func() time.Time { return setAt }

			s.Set(t.Context(), _key, want, tc.ttl)
			assert.Equal(t, tc.wantExpiresAt, s.entries[_key].expiresAt)
		})
	}
}

func TestStore_SetTTLOverwrite(t *testing.T) {
	setAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := []byte("foo")

	s := New()
	s.now = func() time.Time { return setAt }

	s.Set(t.Context(), _key, want, time.Minute)
	assert.Equal(t, setAt.Add(time.Minute), s.entries[_key].expiresAt)

	s.Set(t.Context(), _key, want, _noTTL)
	assert.True(t, s.entries[_key].expiresAt.IsZero())
}
