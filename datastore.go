package nds

import (
	"appengine"
	"appengine/datastore"
	"errors"
	"reflect"
	"sync"
)

const (
	// multiLimit is the App Engine datastore limit for the number of entities
	// that can be PutMulti or GetMulti in one call.
	multiLimit = 1000
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
	cache map[string]*datastore.PropertyList
	sync.RWMutex
}

func NewCacheContext(c appengine.Context) appengine.Context {
	return &cacheContext{
		Context: c,
		cache:   map[string]*datastore.PropertyList{},
	}
}

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

// getMultiCache gets entities from local cache then the datastore.
// dst argument must be a slice.
func getMultiCache(cc *cacheContext,
	keys []*datastore.Key, dst reflect.Value) error {

	cacheMissIndexes := []int{}
	cacheMissKeys := []*datastore.Key{}
	cacheMissDsts := []datastore.PropertyList{}

	errs := make(appengine.MultiError, dst.Len())
	errsNil := true

	// Load from local memory cache.
	cc.RLock()
	for i, key := range keys {
		if pl, ok := cc.cache[key.Encode()]; ok {
			if pl == nil {
				errs[i] = datastore.ErrNoSuchEntity
				errsNil = false
			} else {
				elem := addrValue(dst.Index(i))
				if err := LoadStruct(elem.Interface(), pl); err != nil {
					cc.RUnlock()
					return err
				}
			}
		} else {
			cacheMissIndexes = append(cacheMissIndexes, i)
			cacheMissKeys = append(cacheMissKeys, key)
			cacheMissDsts = append(cacheMissDsts, datastore.PropertyList{})
		}
	}
	cc.RUnlock()

	// Load from datastore.
	if err := datastore.GetMulti(cc, cacheMissKeys, cacheMissDsts); err == nil {
		// Save to local memory cache.
		putMultiLocalCache(cc, cacheMissKeys, cacheMissDsts)

		// Update the callers slice with values.
		for i, index := range cacheMissIndexes {
			pl := cacheMissDsts[i]
			elem := addrValue(dst.Index(index))
			if err := LoadStruct(elem.Interface(), &pl); err != nil {
				return err
			}
		}
	} else if me, ok := err.(appengine.MultiError); ok {
		for i, err := range me {
			if err == nil {
				putLocalCache(cc, cacheMissKeys[i], cacheMissDsts[i])

				// Update the callers slice with values.
				pl := cacheMissDsts[i]
				index := cacheMissIndexes[i]
				elem := addrValue(dst.Index(index))
				if err := LoadStruct(elem.Interface(), &pl); err != nil {
					return err
				}
			} else if err == datastore.ErrNoSuchEntity {
				putLocalCache(cc, cacheMissKeys[i], nil)
				index := cacheMissIndexes[i]
				errs[index] = datastore.ErrNoSuchEntity
				errsNil = false
				// Possibly should zero the callers slice value here.
			} else {
				return err
			}
		}
	} else {
		return err
	}

	if errsNil {
		return nil
	} else {
		return errs
	}
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
	cc.cache[key.Encode()] = &pl
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
