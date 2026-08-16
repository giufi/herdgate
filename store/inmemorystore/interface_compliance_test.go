package inmemorystore_test

import (
	"github.com/giufi/herdgate"
	"github.com/giufi/herdgate/store/inmemorystore"
)

var _ herdgate.Store = (*inmemorystore.Store)(nil)
