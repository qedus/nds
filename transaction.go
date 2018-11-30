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

func (t *Transaction) lockKey(key *datastore.Key) {
	t.lockKeys([]*datastore.Key{key})
}

func (t *Transaction) lockKeys(keys []*datastore.Key) {
	_, lockCacheItems := getCacheLocks(keys)
	t.Lock()
	t.lockCacheItems = append(t.lockCacheItems,
		lockCacheItems...)
	t.Unlock()
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
	return t.tx.Get(key, dst)
}

// GetMulti is a batch version of Get. It bypasses the cache during transactions.
func (t *Transaction) GetMulti(keys []*datastore.Key, dst interface{}) error {
	// We bypass the cache in transactional Get calls
	return t.tx.GetMulti(keys, dst)
}

func (t *Transaction) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	t.lockKey(key)
	return t.tx.Put(key, src)
}

// PutMulti in a batch version of Put. It queues up all keys provided to be locked in the cache.
func (t *Transaction) PutMulti(keys []*datastore.Key, src interface{}) (ret []*datastore.PendingKey, err error) {
	t.lockKeys(keys)
	return t.tx.PutMulti(keys, src)
}

func (t *Transaction) Delete(key *datastore.Key) error {
	t.lockKey(key)
	return t.tx.Delete(key)
}

// DeleteMulti is a batch version of Delete. It queues up all keys provided to be locked in the cache.
func (t *Transaction) DeleteMulti(keys []*datastore.Key) (err error) {
	t.lockKeys(keys)
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
	// tx.Unlock() is not called as the tx context should never be called
	// again so we rather block than allow people to misuse the context.
	t.Lock()
	return t.tx.Rollback()
}

// Query is a helper function to use underlying *datastore.Transaction for queries in nds Transactions
func (t *Transaction) Query(q *datastore.Query) *datastore.Query {
	return q.Transaction(t.tx)
}

// Mutate will lock all keys from the mutations provided.
func (t *Transaction) Mutate(muts ...*Mutation) ([]*datastore.PendingKey, error) {
	mutations := make([]*datastore.Mutation, len(muts))
	keys := make([]*datastore.Key, len(muts))
	for i, mut := range muts {
		mutations[i] = mut.mut
		keys[i] = mut.k
	}
	t.lockKeys(keys)
	return t.tx.Mutate(mutations...)
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
	// again so we rather block than allow people to misuse the context.
	t.Lock()
	return t.c.cacher.SetMulti(t.ctx, t.lockCacheItems)
}
