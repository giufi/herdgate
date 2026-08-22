package herdgate

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestIsNil(t *testing.T) {
	var nilChan chan int
	var nilFunc func()
	var nilMap map[string]int
	var nilPtr *int
	var nilSlice []int
	var nilUnsafePtr unsafe.Pointer
	x := 1

	testCases := map[string]struct {
		value any
		kind  reflect.Kind
		want  bool
	}{
		"untyped nil":            {nil, reflect.Invalid, true},
		"nil chan":               {nilChan, reflect.Chan, true},
		"non-nil chan":           {make(chan int), reflect.Chan, false},
		"nil func":               {nilFunc, reflect.Func, true},
		"non-nil func":           {func() {}, reflect.Func, false},
		"nil map":                {nilMap, reflect.Map, true},
		"non-nil map":            {map[string]int{}, reflect.Map, false},
		"nil pointer":            {nilPtr, reflect.Pointer, true},
		"non-nil pointer":        {&x, reflect.Pointer, false},
		"nil slice":              {nilSlice, reflect.Slice, true},
		"non-nil slice":          {[]int{}, reflect.Slice, false},
		"nil unsafe pointer":     {nilUnsafePtr, reflect.UnsafePointer, true},
		"non-nil unsafe pointer": {unsafe.Pointer(&x), reflect.UnsafePointer, false},
		"non-nilable (int)":      {0, reflect.Int, false},
		"non-nilable (string)":   {"foo", reflect.String, false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.kind, reflect.ValueOf(tc.value).Kind())
			assert.Equal(t, tc.want, isNil(tc.value))
		})
	}
}
