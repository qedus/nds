package nds

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
)

type Transaction struct {
	c   *Client
	ctx context.Context
	tx  *datastore.Transaction
	sync.Mutex
	lockCacheItems []*Item
}

// NewTransaction will start a new datastore.Trnsaction wrapped by nds to properly update the cache
func (c *Client) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (t *Transaction, err error) {
	tx, err := c.ds.NewTransaction(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &Transaction{c: c, ctx: ctx, tx: tx}, nil
}

func (t *Transaction) Get(key *datastore.Key, dst interface{}) error {
	err := t.GetMulti([]*datastore.Key{key}, []interface{}{dst})
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

// GetMulti is a batch version of Get. It bypasses the cache during transactions.
func (t *Transaction) GetMulti(keys []*datastore.Key, dst interface{}) error {
	// We bypass the cache in transactional Get calls
	return t.tx.GetMulti(keys, dst)
}

func (t *Transaction) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	h, err := t.PutMulti([]*datastore.Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(datastore.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return h[0], nil
}

// PutMulti in a batch version of Put. It queues up all keys provided to be locked in the cache.
func (t *Transaction) PutMulti(keys []*datastore.Key, src interface{}) (ret []*datastore.PendingKey, err error) {
	_, lockCacheItems := getCacheLocks(keys)
	t.Lock()
	t.lockCacheItems = append(t.lockCacheItems,
		lockCacheItems...)
	t.Unlock()

	return t.tx.PutMulti(keys, src)
}

func (t *Transaction) Delete(key *datastore.Key) error {
	err := t.DeleteMulti([]*datastore.Key{key})
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti is a batch version of Delete. It queues up all keys provided to be locked in the cache.
func (t *Transaction) DeleteMulti(keys []*datastore.Key) (err error) {
	_, lockCacheItems := getCacheLocks(keys)
	t.Lock()
	t.lockCacheItems = append(t.lockCacheItems,
		lockCacheItems...)
	t.Unlock()

	return t.tx.DeleteMulti(keys)
}

// Commit will commit the cache changes, then commit the transaction
func (t *Transaction) Commit() (*datastore.Commit, error) {
	if err := t.commitCache(); err != nil {
		return nil, err
	}
	return t.tx.Commit()
}

// Rollback is just a passthrough to the underlying datastore.Transaction.
func (t *Transaction) Rollback() (err error) {
	return t.tx.Rollback()
}

// Query is a helper function to use underlying *datastore.Transaction for queries in nds Transactions
func (t *Transaction) RunQuery(q *datastore.Query) *datastore.Query {
	return q.Transaction(t.tx)
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correctly with the cache. You should always use this method for
// transactions if you are using the NDS package.
func (c *Client) RunInTransaction(ctx context.Context, f func(tx *Transaction) error, opts ...datastore.TransactionOption) (cmt *datastore.Commit, err error) {

	return c.ds.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		txn := &Transaction{c: c, ctx: ctx, tx: tx}
		if err := f(txn); err != nil {
			return err
		}

		return txn.commitCache()
	}, opts...)
}

// commitCache will commit the transaction changes to the cache
func (t *Transaction) commitCache() error {
	// tx.Unlock() is not called as the tx context should never be called
	//again so we rather block than allow people to misuse the context.
	t.Lock()
	cacheCtx, err := t.c.cacher.NewContext(t.ctx)
	if err != nil {
		return err
	}
	return t.c.cacher.SetMulti(cacheCtx, t.lockCacheItems)
}
