package herdgate

import (
	"encoding/json"
	"sync/atomic"
)

type fakeCodec struct {
	marshalErr   error
	unmarshalErr error

	marshalPanic   any
	unmarshalPanic any

	marshalCount   atomic.Int64
	unmarshalCount atomic.Int64
}

var _ Codec = (*fakeCodec)(nil)

func (f *fakeCodec) Marshal(v any) ([]byte, error) {
	f.marshalCount.Add(1)
	if f.marshalPanic != nil {
		panic(f.marshalPanic)
	}
	if f.marshalErr != nil {
		return nil, f.marshalErr
	}
	return json.Marshal(v)
}

func (f *fakeCodec) Unmarshal(data []byte, v any) error {
	f.unmarshalCount.Add(1)
	if f.unmarshalPanic != nil {
		panic(f.unmarshalPanic)
	}
	if f.unmarshalErr != nil {
		return f.unmarshalErr
	}
	return json.Unmarshal(data, v)
}
