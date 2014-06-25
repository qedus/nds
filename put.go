package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"math/rand"
	"reflect"
)

// putMultiLimit is the App Engine datastore limit for the maximum number
// of entities that can be put by the datastore.PutMulti at once.
const putMultiLimit = 500

// PutMulti works just like datastore.PutMulti except when a context generated
// from NewContext is used it caches entities in local memory and memcache.
func PutMulti(c appengine.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {

	if err := checkArgs(keys, reflect.ValueOf(vals)); err != nil {
		return nil, err
	}

	if cc, ok := c.(*context); ok {
		return putMulti(cc, keys, vals)
	}
	return datastore.PutMulti(c, keys, vals)
}

// Put is a wrapper around PutMulti. It has the same characteristics as
// datastore.Put.
func Put(c appengine.Context,
	key *datastore.Key, val interface{}) (*datastore.Key, error) {
	k, err := PutMulti(c, []*datastore.Key{key}, []interface{}{val})
	if err != nil {
		return nil, err
	}
	return k[0], nil
}

// putMulti puts the entities into the datastore and then its local cache.
func putMulti(cc *context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {

	lockMemcacheKeys := []string{}
	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		if !key.Incomplete() {
			memcacheKey := createMemcacheKey(key)
			lockMemcacheKeys = append(lockMemcacheKeys, memcacheKey)

			item := &memcache.Item{
				Key:        memcacheKey,
				Flags:      rand.Uint32(),
				Value:      memcacheLock,
				Expiration: memcacheLockTime,
			}
			lockMemcacheItems = append(lockMemcacheItems, item)
		}
	}
	if err := memcache.SetMulti(cc, lockMemcacheItems); err != nil {
		return nil, err
	}

	// Save to the datastore.
	keys, err := datastore.PutMulti(cc, keys, vals)
	if err != nil {
		return nil, err
	}

	if !cc.inTransaction {
		// Remove the locks.
		if err := memcache.DeleteMulti(cc, lockMemcacheKeys); err != nil {
			if _, ok := err.(appengine.MultiError); !ok {
				return nil, err
			}
		}
	}
	return keys, nil
}
