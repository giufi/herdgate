package herdgate

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTTLAttr(t *testing.T) {
	testCases := map[string]struct {
		ttl       time.Duration
		wantValue string
	}{
		"zero ttl":     {ttl: 0, wantValue: "never"},
		"negative ttl": {ttl: -time.Second, wantValue: "never"},
		"positive ttl": {ttl: 5 * time.Minute, wantValue: "5m0s"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			want := slog.String("ttl", tc.wantValue)
			assert.Equal(t, want, ttlAttr(tc.ttl))
		})
	}
}
