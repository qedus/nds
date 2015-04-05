package nds

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

type txContext struct {
	context.Context
	lockMemcacheItems []*memcache.Item
}

func transactionContext(c context.Context) (*txContext, bool) {
	txc, ok := c.(*txContext)
	return txc, ok
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correctly with memcache. You should always use this method for
// transactions if you are using the NDS package.
func RunInTransaction(c context.Context, f func(tc context.Context) error,
	opts *datastore.TransactionOptions) error {

	return datastore.RunInTransaction(c, func(tc context.Context) error {
		txc := &txContext{
			Context: tc,
		}
		if err := f(txc); err != nil {
			return err
		}
		return memcacheSetMulti(tc, txc.lockMemcacheItems)
	}, opts)
}
