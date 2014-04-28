package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	// multiLimit is the App Engine datastore limit for the number of entities
	// that can be PutMulti or GetMulti in one call.
	multiLimit = 1000

	// memcachePrefix is the namespace memcache uses to store entities.
	memcachePrefix = "NDS:"

	// memcacheLockTime is the maximum length of time a memcache lock will be
	// held for. 32 seconds is choosen as 30 seconds is the maximum amount of
	// time an underlying datastore call will retry even if the API reports a
	// success to the user.
	memcacheLockTime = 32 * time.Second

	// memcacheLock is the value that is used to lock memcache.
	memcacheLock = uint32(1)
)

var (
	// milMultiError is a convenience slice used to represent a nil error when
	// grouping errors in GetMulti.
	nilMultiError = make(appengine.MultiError, multiLimit)
)

func checkMultiArgs(keys []*datastore.Key, v reflect.Value) error {
	if v.Kind() != reflect.Slice {
		return errors.New("nds: dst is not a slice")
	}

	if len(keys) != v.Len() {
		return errors.New("nds: key and dst slices have different length")
	}

	return nil
}

// GetMulti works just like datastore.GetMulti except it removes the API limit
// of 1000 entities per request by calling datastore.GetMulti as many times as
// required to complete the request.
//
// Increase the datastore timeout if you get datastore_v3: TIMEOUT errors. You
// can do this using
// http://godoc.org/code.google.com/p/appengine-go/appengine#Timeout.
func GetMulti(c appengine.Context,
	keys []*datastore.Key, dst interface{}) error {

	v := reflect.ValueOf(dst)
	if err := checkMultiArgs(keys, v); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	p := len(keys) / multiLimit
	errs := make([]error, p+1)
	wg := sync.WaitGroup{}
	for i := 0; i < p; i++ {
		index := i
		keySlice := keys[i*multiLimit : (i+1)*multiLimit]
		dstSlice := v.Slice(i*multiLimit, (i+1)*multiLimit)

		wg.Add(1)
		go func() {
			errs[index] = datastore.GetMulti(c, keySlice, dstSlice.Interface())
			wg.Done()
		}()
	}

	if len(keys)%multiLimit == 0 {
		errs = errs[:len(errs)-1]
	} else {
		keySlice := keys[p*multiLimit : len(keys)]
		dstSlice := v.Slice(p*multiLimit, len(keys))
		wg.Add(1)
		go func() {
			errs[p] = datastore.GetMulti(c, keySlice, dstSlice.Interface())
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

	groupedErrs := make(appengine.MultiError, 0, len(keys))
	for _, err := range errs {
		if err == nil {
			groupedErrs = append(groupedErrs, nilMultiError...)
		} else if me, ok := err.(appengine.MultiError); ok {
			groupedErrs = append(groupedErrs, me...)
		} else {
			return err
		}
	}
	return groupedErrs[:len(keys)]
}

type cacheContext struct {
	appengine.Context
	cache map[*datastore.Key]datastore.PropertyList
	sync.RWMutex
}

// NewCacheContext returns an appengine.Context that allows GetMultiCache to
// use local memory cache and memcache.
func NewCacheContext(c appengine.Context) appengine.Context {
	return &cacheContext{
		Context: c,
		cache:   map[*datastore.Key]datastore.PropertyList{},
	}
}

// GetMultiCache works like datastore.GetMulti except it tries to retrieve
// data from local memory cache and memcache when a NewCacheContext is used.
func GetMultiCache(c appengine.Context,
	keys []*datastore.Key, dst interface{}) error {

	v := reflect.ValueOf(dst)
	if err := checkMultiArgs(keys, v); err != nil {
		return err
	}

	if cc, ok := c.(*cacheContext); ok {
		return getMultiCache(cc, keys, v)
	} else {
		return datastore.GetMulti(c, keys, dst)
	}
}

func convertToPropertyLists(
	v reflect.Value) ([]datastore.PropertyList, error) {
	pls := make([]datastore.PropertyList, v.Len())
	for i := range pls {
		pl := datastore.PropertyList{}
		elem := addrValue(v.Index(i))
		if err := SaveStruct(elem.Interface(), &pl); err != nil {
			return nil, err
		}
		pls[i] = pl
	}
	return pls, nil
}

func addrValue(v reflect.Value) reflect.Value {
	if v.Kind() != reflect.Ptr {
		return v.Addr()
	} else {
		return v
	}
}

func setVal(index int, vals reflect.Value, pl *datastore.PropertyList) error {
	elem := addrValue(vals.Index(index))
	return LoadStruct(elem.Interface(), pl)
}

type multiState struct {
	keys      []*datastore.Key
	vals      reflect.Value
	errs      appengine.MultiError
	errsCount int

	keyIndex map[*datastore.Key]int

	missingMemoryKeys map[*datastore.Key]bool

	missingMemcacheKeys map[*datastore.Key]bool

	// These are keys someone else has locked.
	lockedMemcacheKeys map[*datastore.Key]bool

	// These are keys we have locked.
	lockedMemcacheItems map[string]*memcache.Item

	missingDatastoreKeys map[*datastore.Key]bool
}

func newMultiState(keys []*datastore.Key, vals reflect.Value) *multiState {
	ms := &multiState{
		keys: keys,
		vals: vals,
		errs: make(appengine.MultiError, vals.Len()),

		keyIndex: make(map[*datastore.Key]int),

		missingMemoryKeys: make(map[*datastore.Key]bool),

		missingMemcacheKeys: make(map[*datastore.Key]bool),
		lockedMemcacheKeys:  make(map[*datastore.Key]bool),

		missingDatastoreKeys: make(map[*datastore.Key]bool),
	}

	for i, key := range keys {
		ms.keyIndex[key] = i
	}
	return ms
}

// getMultiCache attempts to get entities from local cache, memcache, then the
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
// dst argument must be a slice.
func getMultiCache(cc *cacheContext,
	keys []*datastore.Key, dst reflect.Value) error {

	ms := newMultiState(keys, dst)

	if err := loadMemoryCache(cc, ms); err != nil {
		return err
	}

	if err := loadMemcache(cc, ms); err != nil {
		return err
	}

	// Lock memcache while we get new data from the datastore.
	if err := lockMemcache(cc, ms); err != nil {
		return err
	}

	if err := loadDatastore(cc, ms); err != nil {
		return err
	}

	if err := saveMemcache(cc, ms); err != nil {
		return err
	}

	if err := saveMemoryCache(cc, ms); err != nil {
		return err
	}

	if ms.errsCount == 0 {
		return nil
	} else {
		return ms.errs
	}
}

func loadMemoryCache(cc *cacheContext, ms *multiState) error {
	cc.RLock()
	defer cc.RUnlock()

	for index, key := range ms.keys {
		if pl, ok := cc.cache[key]; ok {
			if len(pl) == 0 {
				ms.errs[index] = datastore.ErrNoSuchEntity
				ms.errsCount++
			} else {
				if err := setVal(index, ms.vals, &pl); err != nil {
					return err
				}
			}
		} else {
			ms.errs[index] = datastore.ErrNoSuchEntity
			ms.errsCount++
			ms.missingMemoryKeys[key] = true
		}
	}
	return nil
}

func loadMemcache(cc *cacheContext, ms *multiState) error {

	memcacheKeys := make([]string, 0, len(ms.missingMemoryKeys))
	for key := range ms.missingMemoryKeys {
		memcacheKeys = append(memcacheKeys, createMemcacheKey(key))
	}

	if items, err := memcache.GetMulti(cc, memcacheKeys); err != nil {
		return err
	} else {
		for key := range ms.missingMemoryKeys {
			memcacheKey := createMemcacheKey(key)

			if item, ok := items[memcacheKey]; ok {
				if isItemLocked(item) {
					ms.lockedMemcacheKeys[key] = true
				} else {
					if pl, err := decodePropertyList(item.Value); err != nil {
						return err
					} else {
						index := ms.keyIndex[key]
						if err := setVal(index, ms.vals, &pl); err != nil {
							return err
						}

						ms.errs[index] = nil
						ms.errsCount--
					}
				}
			} else {
				ms.missingMemcacheKeys[key] = true
			}
		}
	}
	return nil
}

func loadDatastore(c appengine.Context, ms *multiState) error {

	keys := make([]*datastore.Key, 0,
		len(ms.missingMemcacheKeys)+len(ms.lockedMemcacheKeys))
	for key := range ms.missingMemcacheKeys {
		keys = append(keys, key)
	}
	for key := range ms.lockedMemcacheKeys {
		keys = append(keys, key)
	}
	pls := make([]datastore.PropertyList,
		len(ms.missingMemcacheKeys)+len(ms.lockedMemcacheKeys))

	if err := datastore.GetMulti(c, keys, pls); err == nil {
		for i, key := range keys {
			index := ms.keyIndex[key]
			if err := setVal(index, ms.vals, &pls[i]); err != nil {
				return err
			}
			ms.errs[index] = nil
			ms.errsCount--
		}
	} else if me, ok := err.(appengine.MultiError); ok {
		for i, err := range me {
			if err == nil {
				key := keys[i]
				index := ms.keyIndex[key]
				if err := setVal(index, ms.vals, &pls[i]); err != nil {
					return err
				}
				ms.errs[index] = nil
				ms.errsCount--
			} else if err == datastore.ErrNoSuchEntity {
				key := keys[i]
				ms.missingDatastoreKeys[key] = true
			} else {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

func saveMemcache(c appengine.Context, ms *multiState) error {

	items := []*memcache.Item{}
	for key := range ms.missingMemcacheKeys {
		memcacheKey := createMemcacheKey(key)
		if !ms.missingDatastoreKeys[key] {
			index := ms.keyIndex[key]
			s := addrValue(ms.vals.Index(index))
			pl := datastore.PropertyList{}
			if err := SaveStruct(s.Interface(), &pl); err != nil {
				return err
			}

			data, err := encodePropertyList(pl)
			if err != nil {
				return err
			}
			if item, ok := ms.lockedMemcacheItems[memcacheKey]; ok {
				item.Value = data
				item.Flags = 0
				items = append(items, item)
			} else {
				item := &memcache.Item{
					Key:   memcacheKey,
					Value: data,
				}
				items = append(items, item)
			}
		}
	}
	if err := memcache.CompareAndSwapMulti(
		c, items); err == memcache.ErrCASConflict {
		return nil
	} else if err == memcache.ErrNotStored {
		return nil
	} else {
		return err
	}
}

func saveMemoryCache(cc *cacheContext, ms *multiState) error {
	cc.Lock()
	defer cc.Unlock()
	for i, err := range ms.errs {
		if err == nil {
			s := addrValue(ms.vals.Index(i))
			pl := datastore.PropertyList{}
			if err := SaveStruct(s.Interface(), &pl); err != nil {
				return err
			}
			cc.cache[ms.keys[i]] = pl
		}
	}
	return nil
}

func isItemLocked(item *memcache.Item) bool {
	return item.Flags == memcacheLock
}

func lockMemcache(c appengine.Context, ms *multiState) error {

	lockItems := make([]*memcache.Item, 0, len(ms.missingMemcacheKeys))
	memcacheKeys := make([]string, 0, len(ms.missingMemcacheKeys))
	for key := range ms.missingMemcacheKeys {
		memcacheKey := createMemcacheKey(key)
		memcacheKeys = append(memcacheKeys, memcacheKey)

		item := &memcache.Item{
			Key:        memcacheKey,
			Flags:      memcacheLock,
			Value:      []byte{},
			Expiration: memcacheLockTime,
		}
		lockItems = append(lockItems, item)
	}
	if err := memcache.SetMulti(c, lockItems); err != nil {
		return err
	}

	if items, err := memcache.GetMulti(c, memcacheKeys); err != nil {
		return err
	} else {
		ms.lockedMemcacheItems = items
	}
	return nil
}

func decodePropertyList(data []byte) (datastore.PropertyList, error) {
	pl := datastore.PropertyList{}
	return pl, gob.NewDecoder(bytes.NewBuffer(data)).Decode(&pl)
}

func encodePropertyList(pl datastore.PropertyList) ([]byte, error) {
	b := &bytes.Buffer{}
	err := gob.NewEncoder(b).Encode(pl)
	return b.Bytes(), err
}

func createMemcacheKey(key *datastore.Key) string {
	return fmt.Sprintf("%s%s", memcachePrefix, key.Encode())
}

func PutMultiCache(c appengine.Context,
	keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {

	v := reflect.ValueOf(src)
	if err := checkMultiArgs(keys, v); err != nil {
		return nil, err
	}

	if cc, ok := c.(*cacheContext); ok {
		if pls, err := convertToPropertyLists(v); err != nil {
			return nil, err
		} else {
			return putMultiCache(cc, keys, pls)
		}
	} else {
		return datastore.PutMulti(c, keys, src)
	}
}

// putMultiCache puts the entities into the datastore and then its local cache.
func putMultiCache(cc *cacheContext,
	keys []*datastore.Key,
	pls []datastore.PropertyList) ([]*datastore.Key, error) {

	// Save to the datastore.
	completeKeys, err := datastore.PutMulti(cc, keys, pls)
	if err != nil {
		return nil, err
	}

	// Save to local memory cache.
	putMultiLocalCache(cc, completeKeys, pls)

	return completeKeys, nil
}

func putLocalCache(cc *cacheContext,
	key *datastore.Key, pl datastore.PropertyList) {
	cc.Lock()
	cc.cache[key] = pl
	cc.Unlock()
}

func putMultiLocalCache(cc *cacheContext,
	keys []*datastore.Key, pls []datastore.PropertyList) {
	for i, key := range keys {
		putLocalCache(cc, key, pls[i])
	}
}

// SaveStruct saves src to a datastore.PropertyList.
func SaveStruct(src interface{}, pl *datastore.PropertyList) error {
	c, err := make(chan datastore.Property), make(chan error)
	go func() {
		err <- datastore.SaveStruct(src, c)
	}()
	for p := range c {
		*pl = append(*pl, p)
	}
	return <-err
}

// LoadStruct loads a datastore.PropertyList into dst.
func LoadStruct(dst interface{}, pl *datastore.PropertyList) error {
	c := make(chan datastore.Property)
	go func() {
		for _, p := range *pl {
			c <- p
		}
		close(c)
	}()
	return datastore.LoadStruct(dst, c)
}
