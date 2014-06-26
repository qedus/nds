package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/qedus/mcache"
	"math/rand"
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
		dstSlice := v.Slice(lo, hi)

		go func() {
			if inTransaction(c) {
				errs[index] = getMultiTx(c, keySlice, dstSlice.Interface())
			} else {
				errs[index] = getMulti(c, keySlice, dstSlice)
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
func Get(c appengine.Context, key *datastore.Key, val interface{}) error {
	err := GetMulti(c, []*datastore.Key{key}, []interface{}{val})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

type getMultiState struct {
	keys      []*datastore.Key
	vals      reflect.Value
	errs      appengine.MultiError
	errsExist bool

	keyIndex     map[*datastore.Key]int
	memcacheKeys map[string]*datastore.Key

	missingMemcacheKeys map[*datastore.Key]bool

	// These are keys someone else has locked.
	lockedMemcacheKeys map[*datastore.Key]bool

	// These are keys we have locked.
	lockedMemcacheItems map[*datastore.Key]*memcache.Item

	missingDatastoreKeys map[*datastore.Key]bool
}

func newGetMultiState(keys []*datastore.Key, v reflect.Value) *getMultiState {
	gs := &getMultiState{
		keys: keys,
		vals: v,
		errs: make(appengine.MultiError, v.Len()),

		keyIndex:     make(map[*datastore.Key]int),
		memcacheKeys: make(map[string]*datastore.Key),

		missingMemcacheKeys: make(map[*datastore.Key]bool),
		lockedMemcacheKeys:  make(map[*datastore.Key]bool),

		lockedMemcacheItems: make(map[*datastore.Key]*memcache.Item),

		missingDatastoreKeys: make(map[*datastore.Key]bool),
	}

	for i, key := range keys {
		gs.keyIndex[key] = i
		memcacheKey := createMemcacheKey(key)
		gs.memcacheKeys[memcacheKey] = key
	}
	return gs
}

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
// dst argument must be a slice.
func getMulti(c appengine.Context, keys []*datastore.Key, dst reflect.Value) error {

	gs := newGetMultiState(keys, dst)

	if err := loadMemcache(c, gs); err != nil {
		return err
	}

	// Lock memcache while we get new data from the datastore.
	if err := lockMemcache(c, gs); err != nil {
		return err
	}

	if err := loadDatastore(c, gs); err != nil {
		return err
	}

	if err := saveMemcache(c, gs); err != nil {
		return err
	}

	if gs.errsExist {
		return gs.errs
	}
	return nil
}

func getMultiTx(c appengine.Context,
	keys []*datastore.Key, vals interface{}) error {
	return datastore.GetMulti(c, keys, vals)
}

func loadMemcache(c appengine.Context, gs *getMultiState) error {

	memcacheKeys := make([]string, len(gs.keys))
	for i, key := range gs.keys {
		memcacheKeys[i] = createMemcacheKey(key)
	}

	items, err := mcache.GetMulti(c, memcacheKeys)
	me, ok := err.(appengine.MultiError)
	if !ok {
		return err
	}

	for i, key := range gs.keys {
		if me[i] == nil {
			item := items[i]
			if isItemLocked(item) {
				gs.lockedMemcacheKeys[key] = true
			} else {
				pl, err := decodePropertyList(item.Value)
				if err != nil {
					return err
				}
				if err := setValue(i, gs.vals, &pl); err != nil {
					return err
				}
			}
		} else if me[i] == memcache.ErrCacheMiss {
			gs.missingMemcacheKeys[key] = true
		} else {
			return err
		}
	}
	return nil
}

func lockMemcache(c appengine.Context, gs *getMultiState) error {

	lockItems := make([]*memcache.Item, 0, len(gs.missingMemcacheKeys))
	for key := range gs.missingMemcacheKeys {
		memcacheKey := createMemcacheKey(key)

		item := &memcache.Item{
			Key:        memcacheKey,
			Flags:      rand.Uint32(),
			Value:      memcacheLock,
			Expiration: memcacheLockTime,
		}
		lockItems = append(lockItems, item)
	}

	err := mcache.AddMulti(c, lockItems)
	me, ok := err.(appengine.MultiError)
	if !ok {
		return err
	}

	memcacheKeys := make([]string, 0, len(lockItems))
	addedItems := make([]*memcache.Item, 0, len(lockItems))
	for i, item := range lockItems {
		if me[i] == nil {
			memcacheKeys = append(memcacheKeys, item.Key)
			addedItems = append(addedItems, item)
		} else if me[i] == memcache.ErrNotStored {
			key := gs.memcacheKeys[item.Key]
			gs.lockedMemcacheKeys[key] = true
		} else {
			return err
		}
	}

	items, err := mcache.GetMulti(c, memcacheKeys)
	me, ok = err.(appengine.MultiError)
	if !ok {
		return err
	}

	for i, item := range items {
		addItem := addedItems[i]
		if me[i] == nil {
			key := gs.memcacheKeys[item.Key]
			if isItemLocked(item) && item.Flags == addItem.Flags {
				gs.lockedMemcacheItems[key] = item
			} else {
				gs.lockedMemcacheKeys[key] = true
			}
		} else if me[i] == memcache.ErrCacheMiss {
			key := gs.memcacheKeys[item.Key]
			gs.missingMemcacheKeys[key] = true
		} else {
			return err
		}
	}
	return nil
}

func loadDatastore(c appengine.Context, gs *getMultiState) error {
	keysLength := len(gs.missingMemcacheKeys) + len(gs.lockedMemcacheKeys)
	keys := make([]*datastore.Key, 0, keysLength)
	for key := range gs.missingMemcacheKeys {
		keys = append(keys, key)
	}
	for key := range gs.lockedMemcacheKeys {
		keys = append(keys, key)
	}
	pls := make([]datastore.PropertyList, keysLength)

	me := make(appengine.MultiError, len(keys))
	err := datastore.GetMulti(c, keys, pls)
	if e, ok := err.(appengine.MultiError); ok {
		me = e
	} else if err != nil {
		return err
	}

	for i, key := range keys {
		if me[i] == nil {
			index := gs.keyIndex[key]
			if err := setValue(index, gs.vals, &pls[i]); err != nil {
				return err
			}
		} else if me[i] == datastore.ErrNoSuchEntity {
			index := gs.keyIndex[key]
			gs.errs[index] = datastore.ErrNoSuchEntity
			gs.errsExist = true
			gs.missingDatastoreKeys[key] = true
		} else {
			return err
		}
	}

	return nil
}

func saveMemcache(c appengine.Context, gs *getMultiState) error {

	items := []*memcache.Item{}
	for key := range gs.missingMemcacheKeys {
		if !gs.missingDatastoreKeys[key] {
			index := gs.keyIndex[key]
			s := addrValue(gs.vals.Index(index))
			pl := datastore.PropertyList{}
			if err := saveStruct(s.Interface(), &pl); err != nil {
				return err
			}

			data, err := encodePropertyList(pl)
			if err != nil {
				return err
			}

			if item, ok := gs.lockedMemcacheItems[key]; ok {
				item.Value = data
				item.Flags = 0
				items = append(items, item)
			} else {
				item := &memcache.Item{
					Key:   createMemcacheKey(key),
					Value: data,
				}
				items = append(items, item)
			}
		}
	}

	err := mcache.CompareAndSwapMulti(c, items)
	if _, ok := err.(appengine.MultiError); !ok {
		return err
	}
	return nil
}
