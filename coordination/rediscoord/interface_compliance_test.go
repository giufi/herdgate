package rediscoord_test

import (
	"github.com/giufi/herdgate"
	"github.com/giufi/herdgate/coordination/rediscoord"
)

var _ herdgate.Coordination = (*rediscoord.Coordination)(nil)
