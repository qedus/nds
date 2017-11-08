package nds

import (
	"sync"

	"golang.org/x/net/context"
	"cloud.google.com/go/datastore"
	"github.com/bradfitz/gomemcache/memcache"
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
func RunInTransaction(c context.Context, f func(tx *datastore.Transaction) error,
	opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	return dsClient.RunInTransaction(c, func(t *datastore.Transaction) error {
		tx := &transaction{}
		tc := context.WithValue(c, &transactionKey, tx)
		if err := f(t); err != nil {
			return err
		}

		// TODO: check if needed
		// tx.Unlock() is not called as the tx context should never be called
		//again so we rather block than allow people to misuse the context.
		tx.Lock()
		return memcacheSetMulti(tc, tx.lockMemcacheItems)
	}, opts...)
}
