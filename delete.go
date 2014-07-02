package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"errors"
)

// DeleteMulti works just like datastore.DeleteMulti except it maintains
// cache consistency with other NDS methods.
func DeleteMulti(c appengine.Context, keys []*Key) error {
	return deleteMulti(c, keys)
}

func Delete(c appengine.Context, key *Key) error {
	if key == nil {
		return errors.New("nds: key is nil")
	}

	err := deleteMulti(c, []*Key{key})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

func deleteMulti(c appengine.Context, keys []*Key) error {
	// TODO: ensure valid keys.

	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		if key.Incomplete() {
			return ErrInvalidKey
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

	return datastore.DeleteMulti(c, unwrapKeys(keys))
}
