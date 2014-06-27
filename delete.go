package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

// DeleteMulti works just like datastore.DeleteMulti except it maintains
// cache consistency with other NDS methods.
func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	return deleteMulti(c, keys)
}

func deleteMulti(c appengine.Context, keys []*datastore.Key) error {
	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		if key.Incomplete() {
			return datastore.ErrInvalidKey
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
	if err := memcache.SetMulti(c, lockMemcacheItems); err != nil {
		return err
	}

	return datastore.DeleteMulti(c, keys)
}
