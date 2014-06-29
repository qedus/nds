package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"reflect"
	"sync"
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
// vals currently only takes slices of structs. It does not take slices of
// pointers, interfaces or datastore.PropertyLoadSaver.
func GetMulti(c appengine.Context,
	keys []*datastore.Key, vals interface{}) error {

	v := reflect.ValueOf(vals)
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

		index := i
		keySlice := keys[lo:hi]
		valSlice := v.Slice(lo, hi)

		go func() {
			if inTransaction(c) {
				errs[index] = datastore.GetMulti(c,
					keySlice, valSlice.Interface())
			} else {
				errs[index] = getMulti(c, keySlice, valSlice)
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

func Get(c appengine.Context, key *datastore.Key, val interface{}) error {

	if err := checkArgs(key, val); err != nil {
		return err
	}

	vals := reflect.ValueOf([]interface{}{val})
	err := getMulti(c, []*datastore.Key{key}, vals)
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

type cacheState int

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

	sliceType reflect.Type

	item *memcache.Item

	state cacheState
}

// getMulti attempts to get entities from, memcache, then the datastore.
// datastore. It also tries to replenish memcache if needed available. It does
// this in such a way that GetMulti will never get stale results even if the
// function, datastore or server fails at any point. The caching strategy is
// borrowed from Python ndb with some improvements that eliminate some
// consistency issues surrounding ndb, including http://goo.gl/3ByVlA.
func getMulti(c appengine.Context, keys []*datastore.Key,
	vals reflect.Value) error {

	cacheItems := make([]cacheItem, len(keys))
	for i, key := range keys {
		cacheItems[i].key = key
		cacheItems[i].memcacheKey = createMemcacheKey(key)
		cacheItems[i].val = vals.Index(i)
		cacheItems[i].sliceType = vals.Type()
		cacheItems[i].state = miss
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

func loadMemcache(c appengine.Context, cacheItems []cacheItem) error {

	memcacheKeys := make([]string, len(cacheItems))
	for i, cacheItem := range cacheItems {
		memcacheKeys[i] = cacheItem.memcacheKey
	}

	items, err := memcache.GetMulti(c, memcacheKeys)
	if err != nil {
		for i := range cacheItems {
			cacheItems[i].state = externalLock
		}
		c.Warningf("loadMemcache GetMulti %s", err)
		return nil
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
				err := unmarshal(item.Value, cacheItems[i].val)
				if err == nil {
					cacheItems[i].state = done
				} else {
					c.Warningf("loadMemcache unmarshal %s", err)
					cacheItems[i].state = externalLock
				}
			default:
				c.Warningf("loadMemcache unknown item.Flags %d", item.Flags)
				cacheItems[i].state = externalLock
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

	// We don't care if there are errors here.
	if err := memcache.AddMulti(c, lockItems); err != nil {
		c.Warningf("lockMemcache AddMulti %s", err)
	}

	// Get the items again so we can use CAS when updating the cache.
	items, err := memcache.GetMulti(c, lockMemcacheKeys)

	// Cache failed so forget about it and just use the datastore.
	if err != nil {
		for i, cacheItem := range cacheItems {
			if cacheItem.state == miss {
				cacheItems[i].state = externalLock
			}
		}
		c.Warningf("lockMemcache GetMulti %s", err)
		return nil
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
					err := unmarshal(item.Value, cacheItems[i].val)
					if err == nil {
						cacheItems[i].state = done
					} else {
						c.Warningf("lockMemcache unmarshal %s", err)
						cacheItems[i].state = externalLock
					}
				default:
					c.Warningf("lockMemcache unknown item.Flags %d", item.Flags)
					cacheItems[i].state = externalLock
				}
			} else {
				// We just added a memcache item but it now isn't available so
				// treat it as an extarnal lock.
				cacheItems[i].state = externalLock
			}
		}
	}

	return nil
}

func loadDatastore(c appengine.Context, cacheItems []cacheItem) error {

	keys := make([]*datastore.Key, 0, len(cacheItems))
	vals := reflect.MakeSlice(cacheItems[0].sliceType, 0, len(cacheItems))
	cacheItemsIndex := make([]int, 0, len(cacheItems))

	for i, cacheItem := range cacheItems {
		switch cacheItem.state {
		case internalLock, externalLock:
			keys = append(keys, cacheItem.key)
			vals = reflect.Append(vals, cacheItem.val)
			cacheItemsIndex = append(cacheItemsIndex, i)
		}
	}

	var me appengine.MultiError
	if err := datastore.GetMulti(c, keys, vals.Interface()); err == nil {
		me = make(appengine.MultiError, len(keys))
	} else if e, ok := err.(appengine.MultiError); ok {
		me = e
	} else {
		return err
	}

	for i, index := range cacheItemsIndex {
		switch me[i] {
		case nil:
			setValue(cacheItems[index].val, vals.Index(i))

			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = entityItem
				cacheItems[index].item.Expiration = 0
				if data, err := marshal(vals.Index(i)); err == nil {
					cacheItems[index].item.Value = data
				} else {
					cacheItems[index].state = externalLock
					c.Warningf("loadDatastore marshal %s", err)
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

func saveMemcache(c appengine.Context, cacheItems []cacheItem) error {

	saveItems := make([]*memcache.Item, 0, len(cacheItems))
	for _, cacheItem := range cacheItems {
		if cacheItem.state == internalLock {
			saveItems = append(saveItems, cacheItem.item)
		}
	}

	// This is conservative. We could filter out appengine.MultiError and only
	// return other types of errors.
	if err := memcache.CompareAndSwapMulti(c, saveItems); err != nil {
		c.Warningf("saveMemcache CompareAndSwapMulti %s", err)
	}
	return nil
}

func marshal(v reflect.Value) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(v.Interface()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshal(data []byte, v reflect.Value) error {
	v = reflect.Indirect(v)
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() != reflect.Ptr {
		v = v.Addr()
	}
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(v.Interface())
}

func setValue(dst reflect.Value, src reflect.Value) {
	if dst.Kind() == reflect.Struct {
		if src.Kind() != reflect.Struct {
			src = reflect.Indirect(src)
		}
		dst.Set(src)
	} else if dst.Kind() == reflect.Ptr {
		if src.Kind() != reflect.Ptr {
			src = src.Addr()
		}
		dst.Elem().Set(src.Elem())
	}
}
