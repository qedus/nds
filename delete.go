package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"math/rand"
)

// DeleteMulti works just like datastore.DeleteMulti except also cleans up
// local and memcache if a context from NewContext is used.
func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	if cc, ok := c.(*context); ok {
		return deleteMulti(cc, keys)
	}
	return datastore.DeleteMulti(c, keys)
}

// Delete is a wrapper around DeleteMulti.
func Delete(c appengine.Context, key *datastore.Key) error {
	return DeleteMulti(c, []*datastore.Key{key})
}

func deleteMulti(cc *context, keys []*datastore.Key) error {
	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		// TODO: Could possibly check for incomplete key here.
		memcacheKey := createMemcacheKey(key)

		item := &memcache.Item{
			Key:        memcacheKey,
			Flags:      rand.Uint32(),
			Value:      memcacheLock,
			Expiration: memcacheLockTime,
		}
		lockMemcacheItems = append(lockMemcacheItems, item)
	}
	if err := memcache.SetMulti(cc, lockMemcacheItems); err != nil {
		return err
	}

	if err := datastore.DeleteMulti(cc, keys); err != nil {
		return err
	}

	return nil
}
