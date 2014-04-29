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
	"time"
)

const (
	// getMultiLimit is the App Engine datastore limit for the maximum number
	// of entities that can be got by datastore.GetMulti at once.
	// nds.GetMulti increases this limit by performing as many
	// datastore.GetMulti as required concurrently and collating the results.
	getMultiLimit = 1000

	// putMultiLimit is the App Engine datastore limit for the maximum number
	// of entities that can be put by the datastore.PutMulti at once.
	putMultiLimit = 500

	// memcachePrefix is the namespace memcache uses to store entities.
	memcachePrefix = "NDS0:"

	// memcacheLockTime is the maximum length of time a memcache lock will be
	// held for. 32 seconds is choosen as 30 seconds is the maximum amount of
	// time an underlying datastore call will retry even if the API reports a
	// success to the user.
	memcacheLockTime = 32 * time.Second

	// memcacheLock is the value that is used to lock memcache.
	memcacheLock = uint32(1)
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
	keys []*datastore.Key, dst interface{}) error {

	v := reflect.ValueOf(dst)
	if err := checkMultiArgs(keys, v); err != nil {
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

		keySlice := keys[lo:hi]
		dstSlice := v.Slice(lo, hi)

		go func(index int) {
			// Default to datastore.GetMulti if we do not get a nds.context.
			if cc, ok := c.(*context); ok {
				errs[index] = getMulti(cc, keySlice, dstSlice)
			} else {
				errs[index] = datastore.GetMulti(c,
					keySlice, dstSlice.Interface())
			}
			wg.Done()
		}(i)
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
		lo := i * len(errs)
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

type context struct {
	appengine.Context

	// RWMutex is used to protect cache during concurrent access. It needs to
	// be a pointer so it can be copied between transactional and
	// non-transactional contexts when we copy the cache map.
	*sync.RWMutex

	// cache is the memory cache for entities. This could probably be changed
	// to map[string]interface{} in future versions so we don't rely on
	// datastore.PropertyList.
	// The string key is the datastore.Key.Encode() value.
	cache map[string]datastore.PropertyList

	// inTransaction is used to notify our GetMulti, PutMulti and DeleteMulti
	// functions that we are in a transaction as their memory and memcache
	// sync mechanisims change subtly.
	inTransaction bool
}

// NewContext returns an appengine.Context that allows this package to use
// use memory cache and memcache when operation on the datastore.
func NewContext(c appengine.Context) appengine.Context {
	return &context{
		Context: c,
		RWMutex: &sync.RWMutex{},
		cache:   map[string]datastore.PropertyList{},
	}
}

// Get is a wrapper around GetMulti. Its return values are identical to
// datastore.Get.
func Get(c appengine.Context, key *datastore.Key, dst interface{}) error {
	err := GetMulti(c, []*datastore.Key{key}, []interface{}{dst})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

func addrValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Struct {
		return v.Addr()
	}
	return v
}

func setValue(index int, vals reflect.Value, pl *datastore.PropertyList) error {
	elem := addrValue(vals.Index(index))
	return loadStruct(elem.Interface(), pl)
}

type getMultiState struct {
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

func newGetState(keys []*datastore.Key, vals reflect.Value) *getMultiState {
	gs := &getMultiState{
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
		gs.keyIndex[key] = i
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
func getMulti(cc *context, keys []*datastore.Key, dst reflect.Value) error {

	gs := newGetState(keys, dst)

	if err := loadMemory(cc, gs); err != nil {
		return err
	}

	if !cc.inTransaction {
		if err := loadMemcache(cc, gs); err != nil {
			return err
		}

		// Lock memcache while we get new data from the datastore.
		if err := lockMemcache(cc, gs); err != nil {
			return err
		}
	}

	if err := loadDatastore(cc, gs); err != nil {
		return err
	}

	if !cc.inTransaction {
		if err := saveMemcache(cc, gs); err != nil {
			return err
		}
	}

	if err := saveMemory(cc, gs); err != nil {
		return err
	}

	if gs.errsCount == 0 {
		return nil
	}
	return gs.errs
}

func loadMemory(cc *context, gs *getMultiState) error {
	cc.RLock()
	defer cc.RUnlock()

	for index, key := range gs.keys {
		if pl, ok := cc.cache[key.Encode()]; ok {
			if len(pl) == 0 {
				gs.errs[index] = datastore.ErrNoSuchEntity
				gs.errsCount++
			} else {
				if err := setValue(index, gs.vals, &pl); err != nil {
					return err
				}
			}
		} else {
			gs.missingMemoryKeys[key] = true
		}
	}
	return nil
}

func loadMemcache(cc *context, gs *getMultiState) error {

	memcacheKeys := make([]string, 0, len(gs.missingMemoryKeys))
	for key := range gs.missingMemoryKeys {
		memcacheKeys = append(memcacheKeys, createMemcacheKey(key))
	}

	items, err := memcache.GetMulti(cc, memcacheKeys)
	if err != nil {
		return err
	}
	for key := range gs.missingMemoryKeys {
		memcacheKey := createMemcacheKey(key)

		if item, ok := items[memcacheKey]; ok {
			if isItemLocked(item) {
				gs.lockedMemcacheKeys[key] = true
			} else {
				pl, err := decodePropertyList(item.Value)
				if err != nil {
					return err
				}
				index := gs.keyIndex[key]
				if err := setValue(index, gs.vals, &pl); err != nil {
					return err
				}

				gs.errs[index] = nil
				gs.errsCount--
			}
		} else {
			gs.missingMemcacheKeys[key] = true
		}
	}
	return nil
}

func loadDatastore(c appengine.Context, gs *getMultiState) error {

	keys := make([]*datastore.Key, 0,
		len(gs.missingMemoryKeys)+len(gs.lockedMemcacheKeys))
	for key := range gs.missingMemoryKeys {
		keys = append(keys, key)
	}
	for key := range gs.lockedMemcacheKeys {
		keys = append(keys, key)
	}
	pls := make([]datastore.PropertyList,
		len(gs.missingMemoryKeys)+len(gs.lockedMemcacheKeys))

	if err := datastore.GetMulti(c, keys, pls); err == nil {
		for i, key := range keys {
			index := gs.keyIndex[key]
			if err := setValue(index, gs.vals, &pls[i]); err != nil {
				return err
			}
			gs.errs[index] = nil
			gs.errsCount--
		}
	} else if me, ok := err.(appengine.MultiError); ok {
		for i, err := range me {
			if err == nil {
				index := gs.keyIndex[keys[i]]
				if err := setValue(index, gs.vals, &pls[i]); err != nil {
					return err
				}
				gs.errs[index] = nil
				gs.errsCount--
			} else if err == datastore.ErrNoSuchEntity {
				index := gs.keyIndex[keys[i]]
				gs.errs[index] = datastore.ErrNoSuchEntity
				gs.errsCount++
				gs.missingDatastoreKeys[keys[i]] = true
			} else {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

func saveMemcache(c appengine.Context, gs *getMultiState) error {

	items := []*memcache.Item{}
	for key := range gs.missingMemcacheKeys {
		memcacheKey := createMemcacheKey(key)
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
			if item, ok := gs.lockedMemcacheItems[memcacheKey]; ok {
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

func saveMemory(cc *context, gs *getMultiState) error {
	cc.Lock()
	defer cc.Unlock()
	for i, err := range gs.errs {
		if err == nil {
			s := addrValue(gs.vals.Index(i))
			pl := datastore.PropertyList{}
			if err := saveStruct(s.Interface(), &pl); err != nil {
				return err
			}
			cc.cache[gs.keys[i].Encode()] = pl
		}
	}
	return nil
}

func isItemLocked(item *memcache.Item) bool {
	return item.Flags == memcacheLock
}

func lockMemcache(c appengine.Context, gs *getMultiState) error {

	lockItems := make([]*memcache.Item, 0, len(gs.missingMemcacheKeys))
	memcacheKeys := make([]string, 0, len(gs.missingMemcacheKeys))
	for key := range gs.missingMemcacheKeys {
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

	items, err := memcache.GetMulti(c, memcacheKeys)
	if err != nil {
		return err
	}
	gs.lockedMemcacheItems = items

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
	return memcachePrefix + key.Encode()
}

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
			Flags:      memcacheLock,
			Value:      []byte{},
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

	cc.Lock()
	for _, key := range keys {
		delete(cc.cache, key.Encode())
	}
	cc.Unlock()
	return nil
}

// RunInTransaction works just like datastore.RunInTransaction however it
// interacts correcly with memory and memcache if a context generated by
// NewContext is used.
func RunInTransaction(c appengine.Context, f func(tc appengine.Context) error,
	opts *datastore.TransactionOptions) error {

	if cc, ok := c.(*context); ok {
		return runInTransaction(cc, f, opts)
	}
	return datastore.RunInTransaction(c, f, opts)
}

func runInTransaction(cc *context, f func(tc appengine.Context) error,
	opts *datastore.TransactionOptions) error {

	return datastore.RunInTransaction(cc, func(tc appengine.Context) error {
		tcc := &context{
			Context: tc,

			RWMutex: cc.RWMutex,
			cache:   cc.cache,

			inTransaction: true,
		}
		return f(tcc)
	}, opts)
}

// saveStruct saves src to a datastore.PropertyList.
func saveStruct(src interface{}, pl *datastore.PropertyList) error {
	c, err := make(chan datastore.Property), make(chan error)
	go func() {
		err <- datastore.SaveStruct(src, c)
	}()
	for p := range c {
		*pl = append(*pl, p)
	}
	return <-err
}

// loadStruct loads a datastore.PropertyList into dst.
func loadStruct(dst interface{}, pl *datastore.PropertyList) error {
	c := make(chan datastore.Property)
	go func() {
		for _, p := range *pl {
			c <- p
		}
		close(c)
	}()
	return datastore.LoadStruct(dst, c)
}
