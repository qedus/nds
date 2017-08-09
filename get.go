package nds

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

// getMultiLimit is the App Engine datastore limit for the maximum number
// of entities that can be got by datastore.GetMulti at once.
// nds.GetMulti increases this limit by performing as many
// datastore.GetMulti as required concurrently and collating the results.
const getMultiLimit = 1000

// GetMulti works similar to datastore.GetMulti except for two important
// advantages:
//
// 1) It removes the API limit of 1000 entities per request by
// calling the datastore as many times as required to fetch all the keys. It
// does this efficiently and concurrently.
//
// 2) GetMulti function will automatically use memcache where possible before
// accssing the datastore. It uses a caching mechanism similar to the Python
// ndb package. However consistency is improved as NDB consistency issue
// http://goo.gl/3ByVlA is not an issue here or accessing the same key
// concurrently.
//
// If memcache is not working for any reason, GetMulti will default to using
// the datastore without compromising cache consistency.
//
// Important: If you use nds.GetMulti, you must also use the NDS put and delete
// functions in all your code touching the datastore to ensure data consistency.
// This includes using nds.RunInTransaction instead of
// datastore.RunInTransaction.
//
// Increase the datastore timeout if you get datastore_v3: TIMEOUT errors when
// getting thousands of entities. You can do this using
// http://godoc.org/code.google.com/p/appengine-go/appengine#Timeout.
//
// vals must be a []S, []*S, []I or []P, for some struct type S, some interface
// type I, or some non-interface non-pointer type P such that P or *P implements
// datastore.PropertyLoadSaver. If an []I, each element must be a valid dst for
// Get: it must be a struct pointer or implement datastore.PropertyLoadSaver.
//
// As a special case, datastore.PropertyList is an invalid type for dst, even
// though a PropertyList is a slice of structs. It is treated as invalid to
// avoid being mistakenly passed when []datastore.PropertyList was intended.
func GetMulti(c context.Context,
	keys []*datastore.Key, vals interface{}) error {

	v := reflect.ValueOf(vals)
	if err := checkKeysValues(keys, v); err != nil {
		return err
	}

	callCount := (len(keys)-1)/getMultiLimit + 1
	errs := make([]error, callCount)

	var wg sync.WaitGroup
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * getMultiLimit
		hi := (i + 1) * getMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		go func(i int, keys []*datastore.Key, vals reflect.Value) {
			if _, ok := transactionFromContext(c); ok {
				errs[i] = datastoreGetMulti(c, keys, vals.Interface())
			} else {
				errs[i] = getMulti(c, keys, vals)
			}
			wg.Done()
		}(i, keys[lo:hi], v.Slice(lo, hi))
	}
	wg.Wait()

	if isErrorsNil(errs) {
		return nil
	}

	return groupErrors(errs, len(keys), getMultiLimit)
}

// Get loads the entity stored for key into val, which must be a struct pointer.
// Currently PropertyLoadSaver is not implemented. If there is no such entity
// for the key, Get returns ErrNoSuchEntity.
//
// The values of val's unmatched struct fields are not modified, and matching
// slice-typed fields are not reset before appending to them. In particular, it
// is recommended to pass a pointer to a zero valued struct on each Get call.
//
// ErrFieldMismatch is returned when a field is to be loaded into a different
// type than the one it was stored from, or when a field is missing or
// unexported in the destination struct. ErrFieldMismatch is only returned if
// val is a struct pointer.
func Get(c context.Context, key *datastore.Key, val interface{}) error {
	// GetMulti catches nil interface; we need to catch nil ptr here.
	if val == nil {
		return datastore.ErrInvalidEntityType
	}

	err := GetMulti(c, []*datastore.Key{key}, []interface{}{val})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

type cacheState byte

const (
	miss cacheState = iota
	internalLock
	externalLock
	done
)

type cacheItem struct {
	key         *datastore.Key
	memcacheKey string

	val reflect.Value
	err error

	item *memcache.Item

	state cacheState
}

// getMulti attempts to get entities from, memcache, then the datastore. It also
// tries to replenish memcache if needed available. It does this in such a way
// that GetMulti will never get stale results even if the function, datastore or
// server fails at any point. The caching strategy is borrowed from Python ndb
// with improvements that eliminate some consistency issues surrounding ndb,
// including http://goo.gl/3ByVlA.
func getMulti(c context.Context,
	keys []*datastore.Key, vals reflect.Value) error {

	cacheItems := make([]cacheItem, len(keys))
	for i, key := range keys {
		cacheItems[i].key = key
		cacheItems[i].memcacheKey = createMemcacheKey(key)
		cacheItems[i].val = vals.Index(i)
		cacheItems[i].state = miss
	}

	memcacheCtx, err := memcacheContext(c)
	if err != nil {
		return err
	}

	loadMemcache(memcacheCtx, cacheItems)

	lockMemcache(memcacheCtx, cacheItems)

	if err := loadDatastore(c, cacheItems, vals.Type()); err != nil {
		return err
	}

	saveMemcache(memcacheCtx, cacheItems)

	me, errsNil := make(appengine.MultiError, len(cacheItems)), true
	for i, cacheItem := range cacheItems {
		if cacheItem.err != nil {
			me[i] = cacheItem.err
			errsNil = false
		}
	}

	if errsNil {
		return nil
	}
	return me
}

func loadMemcache(c context.Context, cacheItems []cacheItem) {

	memcacheKeys := make([]string, len(cacheItems))
	for i, cacheItem := range cacheItems {
		memcacheKeys[i] = cacheItem.memcacheKey
	}

	items, err := memcacheGetMulti(c, memcacheKeys)
	if err != nil {
		for i := range cacheItems {
			cacheItems[i].state = externalLock
		}
		log.Warningf(c, "nds:loadMemcache GetMulti %s", err)
		return
	}

	for i, memcacheKey := range memcacheKeys {
		if item, ok := items[memcacheKey]; ok {
			switch item.Flags {
			case lockItem:
				cacheItems[i].state = externalLock
			case noneItem:
				cacheItems[i].state = done
				cacheItems[i].err = datastore.ErrNoSuchEntity
			case entityItem:
				pl := datastore.PropertyList{}
				if err := unmarshal(item.Value, &pl); err != nil {
					log.Warningf(c, "nds:loadMemcache unmarshal %s", err)
					cacheItems[i].state = externalLock
					break
				}
				if err := setValue(cacheItems[i].val, pl); err == nil {
					cacheItems[i].state = done
				} else {
					log.Warningf(c, "nds:loadMemcache setValue %s", err)
					cacheItems[i].state = externalLock
				}
			default:
				log.Warningf(c, "nds:loadMemcache unknown item.Flags %d", item.Flags)
				cacheItems[i].state = externalLock
			}
		}
	}
}

// itemLock creates a pseudorandom memcache lock value that enables each call of
// Get/GetMulti to determine if a lock retrieved from memcache is the one it
// created. This is only important when multiple calls of Get/GetMulti are
// performed concurrently for the same previously uncached entity.
func itemLock() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rand.Uint32())
	return b
}

func init() {
	// Seed the pseudorandom number generator to reduce the chance of itemLock
	// collisions.
	rand.Seed(time.Now().UnixNano())
}

func lockMemcache(c context.Context, cacheItems []cacheItem) {

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

	// We don't care if there are errors here.
	if err := memcacheAddMulti(c, lockItems); err != nil {
		log.Warningf(c, "nds:lockMemcache AddMulti %s", err)
	}

	// Get the items again so we can use CAS when updating the cache.
	items, err := memcacheGetMulti(c, lockMemcacheKeys)

	// Cache failed so forget about it and just use the datastore.
	if err != nil {
		for i, cacheItem := range cacheItems {
			if cacheItem.state == miss {
				cacheItems[i].state = externalLock
			}
		}
		log.Warningf(c, "nds:lockMemcache GetMulti %s", err)
		return
	}

	// Cache worked so figure out what items we got.
	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {
			if item, ok := items[cacheItem.memcacheKey]; ok {
				switch item.Flags {
				case lockItem:
					if bytes.Equal(item.Value, cacheItem.item.Value) {
						cacheItems[i].item = item
						cacheItems[i].state = internalLock
					} else {
						cacheItems[i].state = externalLock
					}
				case noneItem:
					cacheItems[i].state = done
					cacheItems[i].err = datastore.ErrNoSuchEntity
				case entityItem:
					pl := datastore.PropertyList{}
					if err := unmarshal(item.Value, &pl); err != nil {
						log.Warningf(c, "nds:lockMemcache unmarshal %s", err)
						cacheItems[i].state = externalLock
						break
					}
					if err := setValue(cacheItems[i].val, pl); err == nil {
						cacheItems[i].state = done
					} else {
						log.Warningf(c, "nds:lockMemcache setValue %s", err)
						cacheItems[i].state = externalLock
					}
				default:
					log.Warningf(c, "nds:lockMemcache unknown item.Flags %d",
						item.Flags)
					cacheItems[i].state = externalLock
				}
			} else {
				// We just added a memcache item but it now isn't available so
				// treat it as an extarnal lock.
				cacheItems[i].state = externalLock
			}
		}
	}
}

func loadDatastore(c context.Context, cacheItems []cacheItem,
	valsType reflect.Type) error {

	keys := make([]*datastore.Key, 0, len(cacheItems))
	vals := make([]datastore.PropertyList, 0, len(cacheItems))
	cacheItemsIndex := make([]int, 0, len(cacheItems))

	for i, cacheItem := range cacheItems {
		switch cacheItem.state {
		case internalLock, externalLock:
			keys = append(keys, cacheItem.key)
			vals = append(vals, datastore.PropertyList{})
			cacheItemsIndex = append(cacheItemsIndex, i)
		}
	}

	var me appengine.MultiError
	if err := datastoreGetMulti(c, keys, vals); err == nil {
		me = make(appengine.MultiError, len(keys))
	} else if e, ok := err.(appengine.MultiError); ok {
		me = e
	} else {
		return err
	}

	for i, index := range cacheItemsIndex {
		switch me[i] {
		case nil:
			pl := vals[i]
			val := cacheItems[index].val
			if err := setValue(val, pl); err != nil {
				cacheItems[index].err = err
			}

			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = entityItem
				cacheItems[index].item.Expiration = 0
				if data, err := marshal(pl); err == nil {
					cacheItems[index].item.Value = data
				} else {
					cacheItems[index].state = externalLock
					log.Warningf(c, "nds:loadDatastore marshal %s", err)
				}
			}
		case datastore.ErrNoSuchEntity:
			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = noneItem
				cacheItems[index].item.Expiration = 0
				cacheItems[index].item.Value = []byte{}
			}
			cacheItems[index].err = datastore.ErrNoSuchEntity
		default:
			cacheItems[index].state = externalLock
			cacheItems[index].err = me[i]
		}
	}
	return nil
}

func saveMemcache(c context.Context, cacheItems []cacheItem) {

	saveItems := make([]*memcache.Item, 0, len(cacheItems))
	for _, cacheItem := range cacheItems {
		if cacheItem.state == internalLock {
			saveItems = append(saveItems, cacheItem.item)
		}
	}

	if err := memcacheCompareAndSwapMulti(c, saveItems); err != nil {
		log.Warningf(c, "nds:saveMemcache CompareAndSwapMulti %s", err)
	}
}
