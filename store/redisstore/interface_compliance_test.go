package redisstore_test

import (
	"github.com/giufi/herdgate"
	"github.com/giufi/herdgate/store/redisstore"
)

var _ herdgate.Store = (*redisstore.Store)(nil)
