package herdgate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONCodec_Marshal(t *testing.T) {
	codec := JSONCodec{}

	data, err := codec.Marshal(jsonPayload{Name: _jsonPayloadName})
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"foo"}`, string(data))
}

func TestJSONCodec_Unmarshal(t *testing.T) {
	codec := JSONCodec{}

	var got jsonPayload
	err := codec.Unmarshal([]byte(`{"name":"foo"}`), &got)
	require.NoError(t, err)
	assert.Equal(t, jsonPayload{Name: _jsonPayloadName}, got)
}

func TestGuardedMarshal_Error(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	codec := &fakeCodec{marshalErr: codecErr}

	data, err := guardedMarshal(t.Context(), codec, jsonPayload{Name: _jsonPayloadName})
	require.ErrorIs(t, err, codecErr)
	require.EqualError(t, err, "marshal herdgate.jsonPayload: generic error")
	assert.Nil(t, data)
}

func TestGuardedMarshal_Panic(t *testing.T) {
	testCases := map[string]struct {
		panicVal   any
		wantErrIs  error
		wantErrMsg string
	}{
		_errValueCase: {
			panicVal:   assert.AnError,
			wantErrIs:  assert.AnError,
			wantErrMsg: "marshal herdgate.jsonPayload panicked: assert.AnError general error for testing",
		},
		_nonErrValueCase: {
			panicVal:   _panicValue,
			wantErrMsg: "marshal herdgate.jsonPayload panicked: generic panic value",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			codec := &fakeCodec{marshalPanic: tc.panicVal}

			data, err := guardedMarshal(t.Context(), codec, jsonPayload{Name: _jsonPayloadName})
			require.Error(t, err)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			}
			require.EqualError(t, err, tc.wantErrMsg)
			assert.Nil(t, data)
		})
	}
}

func TestGuardedUnmarshal_Error(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	codec := &fakeCodec{unmarshalErr: codecErr}

	var decoded jsonPayload
	err := guardedUnmarshal(t.Context(), codec, []byte(`{"name":"foo"}`), &decoded)
	require.ErrorIs(t, err, codecErr)
	assert.EqualError(t, err, "unmarshal *herdgate.jsonPayload: generic error")
}

func TestGuardedUnmarshal_Panic(t *testing.T) {
	testCases := map[string]struct {
		panicVal   any
		wantErrIs  error
		wantErrMsg string
	}{
		_errValueCase: {
			panicVal:   assert.AnError,
			wantErrIs:  assert.AnError,
			wantErrMsg: "unmarshal *herdgate.jsonPayload panicked: assert.AnError general error for testing",
		},
		_nonErrValueCase: {
			panicVal:   _panicValue,
			wantErrMsg: "unmarshal *herdgate.jsonPayload panicked: generic panic value",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			codec := &fakeCodec{unmarshalPanic: tc.panicVal}

			var decoded jsonPayload
			err := guardedUnmarshal(t.Context(), codec, []byte(`{"name":"foo"}`), &decoded)
			require.Error(t, err)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			}
			require.EqualError(t, err, tc.wantErrMsg)
		})
	}
}

type jsonPayload struct {
	Name string `json:"name"`
}
