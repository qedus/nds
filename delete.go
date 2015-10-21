package nds

import (
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

// deleteMultiLimit is the App Engine datastore limit for the maximum number
// of entities that can be deleted by datastore.DeleteMulti at once.
const deleteMultiLimit = 500

// DeleteMulti works just like datastore.DeleteMulti except it maintains
// cache consistency with other NDS methods. It also removes the API limit of
// 500 entities per request by calling the datastore as many times as required
// to put all the keys. It does this efficiently and concurrently.
func DeleteMulti(c context.Context, keys []*datastore.Key) error {

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
			errs[i] = deleteMulti(c, keys)
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
func Delete(c context.Context, key *datastore.Key) error {
	err := deleteMulti(c, []*datastore.Key{key})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

func deleteMulti(c context.Context, keys []*datastore.Key) error {

	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		// Worst case scenario is that we lock the entity for memcacheLockTime.
		// datastore.Delete will raise the appropriate error.
		if key == nil || key.Incomplete() {
			continue
		}

		item := &memcache.Item{
			Key:        createMemcacheKey(key),
			Flags:      lockItem,
			Value:      itemLock(),
			Expiration: memcacheLockTime,
		}
		lockMemcacheItems = append(lockMemcacheItems, item)
	}

	memcacheCtx, err := memcacheContext(c)
	if err != nil {
		return err
	}

	// Make sure we can lock memcache with no errors before deleting.
	if tx, ok := transactionFromContext(c); ok {
		tx.Lock()
		tx.lockMemcacheItems = append(tx.lockMemcacheItems,
			lockMemcacheItems...)
		tx.Unlock()
	} else if err := memcacheSetMulti(memcacheCtx,
		lockMemcacheItems); err != nil {
		return err
	}

	return datastoreDeleteMulti(c, keys)
}
