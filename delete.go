package nds

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
)

// deleteMultiLimit is the Google Cloud Datastore limit for the maximum number
// of entities that can be deleted by datastore.DeleteMulti at once.
// https://cloud.google.com/datastore/docs/concepts/limits
const deleteMultiLimit = 500

// DeleteMulti works just like datastore.DeleteMulti except it maintains
// cache consistency with other NDS methods. It also removes the API limit of
// 500 entities per request by calling the datastore as many times as required
// to put all the keys. It does this efficiently and concurrently.
func (c *Client) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {

	callCount := (len(keys)-1)/deleteMultiLimit + 1
	errs := make([]error, callCount)

	var wg sync.WaitGroup
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * deleteMultiLimit
		hi := (i + 1) * deleteMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		go func(i int, keys []*datastore.Key) {
			errs[i] = c.deleteMulti(ctx, keys)
			wg.Done()
		}(i, keys[lo:hi])
	}
	wg.Wait()

	if isErrorsNil(errs) {
		return nil
	}

	return groupErrors(errs, len(keys), deleteMultiLimit)
}

// Delete deletes the entity for the given key.
func (c *Client) Delete(ctx context.Context, key *datastore.Key) error {
	err := c.deleteMulti(ctx, []*datastore.Key{key})
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

// deleteMulti will batch delete keys by first locking the corresponding items in the
// cache then deleting them from datastore.
func (c *Client) deleteMulti(ctx context.Context, keys []*datastore.Key) error {

	_, lockCacheItems := getCacheLocks(keys)

	// Make sure we can lock the cache with no errors before deleting.
	if err := c.cacher.SetMulti(ctx,
		lockCacheItems); err != nil {
		return err
	}

	return c.ds.DeleteMulti(ctx, keys)
}
