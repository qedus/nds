package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

type txContext struct {
	appengine.Context
	lockMemcacheItems []*memcache.Item
}

func inTransaction(c appengine.Context) bool {
	_, ok := c.(txContext)
	return ok
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correctly with memcache. You should always use this method for
// transactions if you are using the NDS package.
func RunInTransaction(c appengine.Context, f func(tc appengine.Context) error,
	opts *datastore.TransactionOptions) error {

	return datastore.RunInTransaction(c, func(tc appengine.Context) error {
		txc := &txContext{
			Context: tc,
		}
		if err := f(txc); err != nil {
			return err
		}
		return memcacheSetMulti(tc, txc.lockMemcacheItems)
	}, opts)
}
