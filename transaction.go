package nds

import (
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

var transactionKey = "used for *transaction"

type transaction struct {
	sync.Mutex
	lockMemcacheItems []*memcache.Item
}

func transactionFromContext(c context.Context) (*transaction, bool) {
	tx, ok := c.Value(&transactionKey).(*transaction)
	return tx, ok
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correctly with memcache. You should always use this method for
// transactions if you are using the NDS package.
func RunInTransaction(c context.Context, f func(tc context.Context) error,
	opts *datastore.TransactionOptions) error {

	return datastore.RunInTransaction(c, func(tc context.Context) error {
		tx := &transaction{}
		tc = context.WithValue(tc, &transactionKey, tx)
		if err := f(tc); err != nil {
			return err
		}

		// tx.Unlock() is not called as the tx context should never be called
		//again so we rather block than allow people to misuse the context.
		tx.Lock()
		return memcacheSetMulti(tc, tx.lockMemcacheItems)
	}, opts)
}
