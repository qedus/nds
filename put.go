package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"reflect"
)

// putMultiLimit is the App Engine datastore limit for the maximum number
// of entities that can be put by the datastore.PutMulti at once.
const putMultiLimit = 500

// PutMulti works just like datastore.PutMulti except it interacts
// appropriately with NDS's caching strategy.
// Currently, vals can only be slices of struct pointers, []*S.
func PutMulti(c appengine.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {

	if err := checkArgs(keys, reflect.ValueOf(vals)); err != nil {
		return nil, err
	}

	return putMulti(c, keys, vals)
}

// Put is a wrapper around PutMulti.
/*
func Put(c appengine.Context,
	key *datastore.Key, val interface{}) (*datastore.Key, error) {
	k, err := PutMulti(c, []*datastore.Key{key}, []interface{}{val})
	if err != nil {
		return nil, err
	}
	return k[0], nil
}
*/

// putMulti puts the entities into the datastore and then its local cache.
func putMulti(c appengine.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {

	lockMemcacheKeys := make([]string, 0, len(keys))
	lockMemcacheItems := make([]*memcache.Item, 0, len(keys))
	for _, key := range keys {
		if !key.Incomplete() {
			item := &memcache.Item{
				Key:        createMemcacheKey(key),
				Flags:      lockItem,
				Value:      itemLock(),
				Expiration: memcacheLockTime,
			}
			lockMemcacheItems = append(lockMemcacheItems, item)
			lockMemcacheKeys = append(lockMemcacheKeys, item.Key)
		}
	}

	if err := memcache.SetMulti(c, lockMemcacheItems); err != nil {
		return nil, err
	}

	// Save to the datastore.
	keys, err := datastore.PutMulti(c, keys, vals)
	if err != nil {
		return nil, err
	}

	if !inTransaction(c) {
		// Remove the locks.
		if err := memcache.DeleteMulti(c, lockMemcacheKeys); err != nil {
			if _, ok := err.(appengine.MultiError); !ok {
				return nil, err
			}
		}
	}
	return keys, nil
}
