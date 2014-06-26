package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
	"sync"
)

// getMultiLimit is the App Engine datastore limit for the maximum number
// of entities that can be got by datastore.GetMulti at once.
// nds.GetMulti increases this limit by performing as many
// datastore.GetMulti as required concurrently and collating the results.
const getMultiLimit = 1000

// GetMulti works just like datastore.GetMulti except for two important
// advantages:
//
// 1) It removes the API limit of 1000 entities per request by
// calling the datastore as many times as required to fetch all the keys. It
// does this efficiently and concurrently.
//
// 2) If you use an appengine.Context created from this packages NewContext the
// GetMulti function will automatically invoke a caching mechanism identical
// to the Python ndb package. It also has the same strong cache consistency
// guarantees as the Python ndb package. It will check local memory for an
// entity, then check memcache and then the datastore. This has the potential
// to greatly speed up your entity access and reduce Google App Engine costs.
// Note that if you use GetMulti with this packages NewContext, you must do all
// your other datastore accesses with other methods from this package to ensure
// cache consistency.
//
// Increase the datastore timeout if you get datastore_v3: TIMEOUT errors when
// getting thousands of entities. You can do this using
// http://godoc.org/code.google.com/p/appengine-go/appengine#Timeout.
func GetMulti(c appengine.Context,
	keys []*datastore.Key, vals interface{}) error {

	v := reflect.ValueOf(vals)
	if err := checkArgs(keys, v); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	callCount := (len(keys)-1)/getMultiLimit + 1
	errs := make([]error, callCount)

	wg := sync.WaitGroup{}
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * getMultiLimit
		hi := (i + 1) * getMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		index := i
		keySlice := keys[lo:hi]
		valsSlice := v.Slice(lo, hi)

		go func() {
			if inTransaction(c) {
				errs[index] = getMultiTx(c, keySlice, valsSlice.Interface())
			} else {
				errs[index] = getMulti(c, keySlice, valsSlice)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// Quick escape if all errors are nil.
	errsNil := true
	for _, err := range errs {
		if err != nil {
			errsNil = false
		}
	}
	if errsNil {
		return nil
	}

	groupedErrs := make(appengine.MultiError, len(keys))
	for i, err := range errs {
		lo := i * getMultiLimit
		hi := (i + 1) * getMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}
		if me, ok := err.(appengine.MultiError); ok {
			copy(groupedErrs[lo:hi], me)
		} else if err != nil {
			return err
		}
	}
	return groupedErrs
}

// Get is a wrapper around GetMulti. Its return values are identical to
// datastore.Get.
/*
func Get(c appengine.Context, key *datastore.Key, val interface{}) error {
    v := reflect.ValueOf(val)
    sliceType := reflect.SliceOf(v.Type())
    slice := reflect.MakeSlice(sliceType, 1, 1)
    slice.Index(0).Set(v)
    err := getMulti(c, []*datastore.Key{key}, slice)
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}
*/

// getMulti attempts to get entities from local cache, memcache, then the
// datastore. It also tries to replenish each cache in turn if an entity is
// available.
// The not so obvious part is replenishing memcache with the datastore to
// ensure we don't write stale values.
//
// Here's how it works assuming there is nothing in local cache. (Note this is
// taken form Python ndb):
// Firstly get as many entities from memcache as possible. The returned values
// can be in one of three states: No entity, locked value or the acutal entity.
//
// Actual entity case:
// If the value from memcache is an actual entity then replensish the local
// cache and return that entity to the caller.
//
// Locked entity case:
// If the value is locked then just ignore that entity and go to the datastore
// to see if it exists.
//
// No entity case:
// If no entity is returned from memcache then do the following things to ensure
// we don't accidentally update memcache with stale values.
// 1) Lock that entity in memcache by setting memcacheLock on that entities key.
//    Note that the lock timeout is 32 seconds to cater for a datastore edge
//    case which I currently can't quite remember.
// 2) Immediately get that entity back from memcache ensuring the compare and
//    swap ID is set.
// 3) Get the entity from the datastore.
// 4) Set the entity in memcache using compare and swap. If this succeeds then
//    we are guaranteed to have the latest value in memcache. If it fails due
//    to a CAS failure then there must have been a concurrent write to
//    memcache and now the memcache for that key is out of action for 32
//    seconds.
//
// Note that within a transaction, much of this functionality is lost to ensure
// datastore consistency.
//
// vals argument must be a slice.
func getMulti(c appengine.Context, keys []*datastore.Key,
	vals reflect.Value) error {

	cacheItems := make([]cacheItem, len(keys))
	for i, key := range keys {
		cacheItems[i].key = key
		cacheItems[i].memcacheKey = createMemcacheKey(key)
		cacheItems[i].val = vals.Index(i)
	}

	if err := loadMemcache(c, cacheItems); err != nil {
		return err
	}

	// Lock memcache while we get new data from the datastore.
	if err := lockMemcache(c, cacheItems); err != nil {
		return err
	}

	if err := loadDatastore(c, cacheItems); err != nil {
		return err
	}

	if err := saveMemcache(c, cacheItems); err != nil {
		return err
	}

	me, errsNil := make(appengine.MultiError, len(cacheItems)), true
	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {
			me[i] = datastore.ErrNoSuchEntity
			errsNil = false
		}
	}

	if errsNil {
		return nil
	}
	return me
}

func getMultiTx(c appengine.Context,
	keys []*datastore.Key, vals interface{}) error {
	return datastore.GetMulti(c, keys, vals)
}

type cacheState int

const (
	miss cacheState = iota
	present
	internalLock
	externalLock
)

type cacheItem struct {
	key         *datastore.Key
	memcacheKey string

	val reflect.Value

	item *memcache.Item

	state cacheState
}

func loadMemcache(c appengine.Context, cacheItems []cacheItem) error {

	memcacheKeys := make([]string, len(cacheItems))
	for i, cacheItem := range cacheItems {
		memcacheKeys[i] = cacheItem.memcacheKey
	}

	items, err := memcache.GetMulti(c, memcacheKeys)
	if err != nil {
		return err
	}

	for i, memcacheKey := range memcacheKeys {
		if item, ok := items[memcacheKey]; ok {
			if isItemLocked(item) {
				cacheItems[i].state = externalLock
			} else {
				cacheItems[i].item = item
				cacheItems[i].state = present
				err := unmarshal(item.Value,
					cacheItems[i].val.Addr().Interface())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func lockMemcache(c appengine.Context, cacheItems []cacheItem) error {

	lockItems := make([]*memcache.Item, 0, len(cacheItems))
	lockMemcacheKeys := make([]string, 0, len(cacheItems))
	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {

			item := &memcache.Item{
				Key:        cacheItem.memcacheKey,
				Flags:      lockItem,
				Value:      itemLock(),
				Expiration: memcacheLockTime,
			}
			cacheItems[i].item = item
			lockItems = append(lockItems, item)
			lockMemcacheKeys = append(lockMemcacheKeys, cacheItem.memcacheKey)
		}
	}

	// This is currently conservative behavoiur. We could see if a multi error
	// is returned, loop through it and mark any items appropriately that were
	// that were not added as locked.
	if err := memcache.AddMulti(c, lockItems); err != nil {
		return err
	}

	items, err := memcache.GetMulti(c, lockMemcacheKeys)
	if err != nil {
		return err
	}

	// This is currently conservative behaviour. We could set states to
	// present and miss.
	if len(lockMemcacheKeys) != len(items) {
		return errors.New("nds: not all memcache locks obtained")
	}

	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {
			item := items[cacheItem.memcacheKey]
			if isItemLocked(item) {
				if item.Flags == cacheItem.item.Flags {
					cacheItems[i].item = item
					cacheItems[i].state = internalLock
				} else {
					cacheItems[i].item = nil
					cacheItems[i].state = externalLock
				}
			} else {
				cacheItems[i].item = item
				cacheItems[i].state = present
			}
		}
	}

	return nil
}

func loadDatastore(c appengine.Context, cacheItems []cacheItem) error {
	keys := make([]*datastore.Key, 0, len(cacheItems))
	cacheItemsIndex := make([]int, 0, len(cacheItems))
	for i, cacheItem := range cacheItems {
		if cacheItem.state == internalLock || cacheItem.state == externalLock {
			keys = append(keys, cacheItem.key)
			cacheItemsIndex = append(cacheItemsIndex, i)
		}
	}

	elemType := cacheItems[0].val.Type()
	sliceType := reflect.SliceOf(elemType)
	vals := reflect.MakeSlice(sliceType, len(keys), len(keys))

	var me appengine.MultiError
	if err := datastore.GetMulti(c, keys, vals.Interface()); err == nil {
		me = make(appengine.MultiError, len(keys))
	} else if e, ok := err.(appengine.MultiError); ok {
		me = e
	} else {
		return err
	}

	for i, index := range cacheItemsIndex {
		if me[i] == nil {
			cacheItems[index].val.Set(vals.Index(i))
		} else if me[i] == datastore.ErrNoSuchEntity {
			cacheItems[index].state = miss
		} else {
			return me[i]
		}
	}

	return nil
}

func saveMemcache(c appengine.Context, cacheItems []cacheItem) error {

	saveItems := make([]*memcache.Item, 0, len(cacheItems))
	for _, cacheItem := range cacheItems {
		if cacheItem.state == internalLock {
			value, err := marshal(cacheItem.val.Interface())
			if err != nil {
				return err
			}
			item := cacheItem.item
			item.Flags = entityItem
			item.Value = value
			saveItems = append(saveItems, item)
		}
	}

	// This is conservative. We could filter out appengine.MultiError and only
	// return other types of errors.
	return memcache.CompareAndSwapMulti(c, saveItems)
}

func marshal(v interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshal(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(v)
}
