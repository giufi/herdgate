package herdgate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const key = "key"

func TestHerdgate_DoCacheMiss(t *testing.T) {
	want := []byte("foo")

	testCases := map[string]time.Duration{
		"positive TTL": time.Minute,
		"zero TTL":     0,
		"negative TTL": -time.Minute,
	}

	for name, ttl := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			hg := &Herdgate{store: store}

			calls := 0
			fetch := func() ([]byte, error) {
				calls++
				return want, nil
			}

			got, err := hg.Do(key, ttl, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, 1, calls)
			assert.Equal(t, []string{key}, store.getCalls)
			assert.Equal(t, []setCall{{key: key, value: want, ttl: ttl}}, store.setCalls)
		})
	}
}

func TestHerdgate_DoCacheHit(t *testing.T) {
	want := []byte("cached")
	store := &fakeStore{getValue: want, getFound: true}
	hg := &Herdgate{store: store}

	calls := 0
	fetch := func() ([]byte, error) {
		calls++
		return []byte("should not be called"), nil
	}

	got, err := hg.Do(key, time.Minute, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 0, calls)
	assert.Equal(t, []string{key}, store.getCalls)
	assert.Empty(t, store.setCalls)
}

func TestHerdgate_DoFetchError(t *testing.T) {
	store := &fakeStore{}
	hg := &Herdgate{store: store}

	wantErr := assert.AnError
	fetch := func() ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(key, time.Minute, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
	assert.Equal(t, []string{key}, store.getCalls)
	assert.Empty(t, store.setCalls)
}
