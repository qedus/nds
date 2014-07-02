package nds

import (
	"appengine"
	"appengine/datastore"
)

type txContext struct {
	appengine.Context
}

func inTransaction(c appengine.Context) bool {
	_, ok := c.(txContext)
	return ok
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correcly with memcache. You should always use this method for
// transactions if you are using the NDS package.
func RunInTransaction(c appengine.Context, f func(tc appengine.Context) error,
	opts *TransactionOptions) error {

	txOpts := &datastore.TransactionOptions{XG: opts.XG}

	return datastore.RunInTransaction(c, func(tc appengine.Context) error {
		txc := txContext{
			Context: tc,
		}
		return f(txc)
	}, txOpts)
}

type TransactionOptions struct {
	XG bool
}
