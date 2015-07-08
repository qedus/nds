package nds

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

// DeleteMulti works just like datastore.DeleteMulti except it maintains
// cache consistency with other NDS methods.
func DeleteMulti(c context.Context, keys []*datastore.Key) error {
	return deleteMulti(c, keys)
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

	// Make sure we can lock memcache with no errors before deleting.
	if tx, ok := transactionFromContext(c); ok {
		tx.Lock()
		tx.lockMemcacheItems = append(tx.lockMemcacheItems,
			lockMemcacheItems...)
		tx.Unlock()
	} else if err := memcacheSetMulti(c, lockMemcacheItems); err != nil {
		return err
	}

	return datastoreDeleteMulti(c, keys)
}
