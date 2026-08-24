package herdgate

import "encoding/json"

type fakeCodec struct {
	marshalErr   error
	unmarshalErr error

	marshalPanic   any
	unmarshalPanic any

	marshalCalls   int
	unmarshalCalls int
}

var _ Codec = (*fakeCodec)(nil)

func (f *fakeCodec) Marshal(v any) ([]byte, error) {
	f.marshalCalls++
	if f.marshalPanic != nil {
		panic(f.marshalPanic)
	}
	if f.marshalErr != nil {
		return nil, f.marshalErr
	}
	return json.Marshal(v)
}

func (f *fakeCodec) Unmarshal(data []byte, v any) error {
	f.unmarshalCalls++
	if f.unmarshalPanic != nil {
		panic(f.unmarshalPanic)
	}
	if f.unmarshalErr != nil {
		return f.unmarshalErr
	}
	return json.Unmarshal(data, v)
}
