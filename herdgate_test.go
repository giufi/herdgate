package herdgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDo_CacheMiss(t *testing.T) {
	hg := New()
	calls := 0
	want := []byte("foo")
	fetch := func() ([]byte, error) {
		calls++
		return want, nil
	}

	got, err := hg.Do("key", fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, calls)
}

func TestDo_CacheHit(t *testing.T) {
	hg := New()
	key := "key"
	calls := 0
	fetch := func() ([]byte, error) {
		calls++
		return fmt.Appendf(nil, "foo-%d", calls), nil
	}

	first, err := hg.Do(key, fetch)
	require.NoError(t, err)
	assert.Equal(t, []byte("foo-1"), first)
	assert.Equal(t, 1, calls)

	second, err := hg.Do(key, fetch)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, 1, calls)
}

func TestDo_FetchError(t *testing.T) {
	hg := New()
	key := "key"
	wantErr := assert.AnError
	erroringFetch := func() ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(key, erroringFetch)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)

	calls := 0
	want := []byte("foo")
	fetch := func() ([]byte, error) {
		calls++
		return want, nil
	}

	got, err = hg.Do(key, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, calls)
}
