package inmemorystore

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestStore_GetMiss(t *testing.T) {
	otherKey := "otherKey"

	s := New()
	s.Set(t.Context(), otherKey, []byte("foo"), _noTTL)

	got, ok := s.Get(t.Context(), _key)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestStore_GetTypeMismatch(t *testing.T) {
	s := New()
	s.entries.m.Store(_key, "not a storeEntry")

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
			require.True(t, ok)
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
			_, stillStored := s.entries.Load(t.Context(), _key)
			assert.True(t, stillStored)
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
			entry, ok := s.entries.Load(t.Context(), _key)
			require.True(t, ok)
			assert.Equal(t, tc.wantExpiresAt, entry.expiresAt)
		})
	}
}

func TestStore_SetTTLOverwrite(t *testing.T) {
	setAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := []byte("foo")

	s := New()
	s.now = func() time.Time { return setAt }

	s.Set(t.Context(), _key, want, time.Minute)
	entry, ok := s.entries.Load(t.Context(), _key)
	require.True(t, ok)
	assert.Equal(t, setAt.Add(time.Minute), entry.expiresAt)

	s.Set(t.Context(), _key, want, _noTTL)
	entry, ok = s.entries.Load(t.Context(), _key)
	require.True(t, ok)
	assert.True(t, entry.expiresAt.IsZero())
}

func TestStore_GetIndependentCopy(t *testing.T) {
	s := New()
	s.Set(t.Context(), _key, []byte("foo"), _noTTL)

	got, ok := s.Get(t.Context(), _key)
	require.True(t, ok)
	got[0] = 'X'

	got, ok = s.Get(t.Context(), _key)
	assert.True(t, ok)
	assert.Equal(t, []byte("foo"), got)
}

func TestStore_SetIndependentCopy(t *testing.T) {
	original := []byte("foo")

	s := New()
	s.Set(t.Context(), _key, original, _noTTL)
	original[0] = 'X'

	got, ok := s.Get(t.Context(), _key)
	assert.True(t, ok)
	assert.Equal(t, []byte("foo"), got)
}

// This test relies on go test -race to catch data races.
func TestStore_ConcurrentAccess(t *testing.T) {
	s := New()

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			s.Set(t.Context(), _key, []byte("value"), _noTTL)
			s.Get(t.Context(), _key)
		})
	}
	wg.Wait()
}
