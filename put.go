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

// PutMulti works just like datastore.PutMulti except when a context generated
// from NewContext is used it caches entities in local memory and memcache.
func PutMulti(c appengine.Context,
	keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {

	v := reflect.ValueOf(src)
	if err := checkMultiArgs(keys, v); err != nil {
		return nil, err
	}

	if cc, ok := c.(*context); ok {
		return putMulti(cc, keys, v)
	}
	return datastore.PutMulti(c, keys, src)
}

// Put is a wrapper around PutMulti. It has the same characteristics as
// datastore.Put.
func Put(c appengine.Context,
	key *datastore.Key, src interface{}) (*datastore.Key, error) {
	k, err := PutMulti(c, []*datastore.Key{key}, []interface{}{src})
	if err != nil {
		return nil, err
	}
	return k[0], nil
}

// putMulti puts the entities into the datastore and then its local cache.
//
// Warning that errors still need to be sorted out here so that if an error is
// returned we must be sure that the data did not commit to the datastore. For
// example, we could convert the src to property lists right at the beginning
// of the function or we could get rid of the reliance on propertly lists
// completely.
func putMulti(cc *context,
	keys []*datastore.Key, src reflect.Value) ([]*datastore.Key, error) {

	lockMemcacheKeys := []string{}
	lockMemcacheItems := []*memcache.Item{}
	for _, key := range keys {
		if !key.Incomplete() {
			memcacheKey := createMemcacheKey(key)
			lockMemcacheKeys = append(lockMemcacheKeys, memcacheKey)

			item := &memcache.Item{
				Key:        memcacheKey,
				Flags:      memcacheLock,
				Value:      []byte{},
				Expiration: memcacheLockTime,
			}
			lockMemcacheItems = append(lockMemcacheItems, item)
		}
	}
	if err := memcache.SetMulti(cc, lockMemcacheItems); err != nil {
		return nil, err
	}

	// Save to the datastore.
	keys, err := datastore.PutMulti(cc, keys, src.Interface())
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

	// Save to local memory cache.
	cc.Lock()
	defer cc.Unlock()
	for i, key := range keys {
		pl := datastore.PropertyList{}
		elem := addrValue(src.Index(i))
		if err := saveStruct(elem.Interface(), &pl); err != nil {
			return nil, err
		}
		cc.cache[key.Encode()] = pl
	}

	return keys, nil
}
