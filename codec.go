package herdgate

import (
	"context"
	"encoding/json"
	"fmt"
)

// Codec is the interface for converting a value to and from bytes.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is a [Codec] that marshals and unmarshals a value as JSON.
type JSONCodec struct{}

var _ Codec = JSONCodec{}

func (JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// guardedMarshal encodes v with codec, turning a panic into an error.
func guardedMarshal(ctx context.Context, codec Codec, v any) (_ []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("herdgate: marshal %T panicked: %w", v, errFromPanic(ctx, r))
		}
	}()

	data, err := codec.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", v, err)
	}
	return data, nil
}

// guardedUnmarshal decodes data into v with codec, turning a panic into an error.
func guardedUnmarshal(ctx context.Context, codec Codec, data []byte, v any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("herdgate: unmarshal %T panicked: %w", v, errFromPanic(ctx, r))
		}
	}()

	if err := codec.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshal %T: %w", v, err)
	}
	return nil
}
