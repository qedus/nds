package nds

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

const transactionKey = 0

type transaction struct {
	lockMemcacheItems []*memcache.Item
}

func transactionFromContext(c context.Context) (*transaction, bool) {
	tx, ok := c.Value(transactionKey).(*transaction)
	return tx, ok
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correctly with memcache. You should always use this method for
// transactions if you are using the NDS package.
func RunInTransaction(c context.Context, f func(tc context.Context) error,
	opts *datastore.TransactionOptions) error {

	return datastore.RunInTransaction(c, func(tc context.Context) error {
		tx := &transaction{}
		tc = context.WithValue(tc, transactionKey, tx)
		if err := f(tc); err != nil {
			return err
		}
		return memcacheSetMulti(tc, tx.lockMemcacheItems)
	}, opts)
}
