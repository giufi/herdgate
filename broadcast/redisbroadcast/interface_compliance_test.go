package redisbroadcast_test

import (
	"github.com/giufi/herdgate"
	"github.com/giufi/herdgate/broadcast/redisbroadcast"
)

var _ herdgate.Broadcast = (*redisbroadcast.Broadcast)(nil)
