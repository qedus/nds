package nds

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// getMultiLimit is the Google Cloud Datastore limit for the maximum number
// of entities that can be got by datastore.GetMulti at once.
// nds.GetMulti increases this limit by performing as many
// datastore.GetMulti as required concurrently and collating the results.
// https://cloud.google.com/datastore/docs/concepts/limits
const getMultiLimit = 1000

var (
	// getMultiHook exists purely for testing
	getMultiHook func(ctx context.Context, keys []*datastore.Key, vals interface{}) error
)

// GetMulti works similar to datastore.GetMulti except for two important
// advantages:
//
// 1) It removes the API limit of 1000 entities per request by
// calling the datastore as many times as required to fetch all the keys. It
// does this efficiently and concurrently.
//
// 2) GetMulti function will automatically use the cache where possible before
// accssing the datastore. It uses a caching mechanism similar to the Python
// ndb package. However consistency is improved as NDB consistency issue
// http://goo.gl/3ByVlA is not an issue here or accessing the same key
// concurrently.
//
// If the cache is not working for any reason, GetMulti will default to using
// the datastore without compromising cache consistency.
//
// Important: If you use nds.GetMulti, you must also use the NDS put and delete
// functions in all your code touching the datastore to ensure data consistency.
// This includes using nds.RunInTransaction instead of
// datastore.RunInTransaction.
//
// Increase the datastore timeout if you get datastore_v3: TIMEOUT errors when
// getting thousands of entities. You can do this using context.WithTimeout
// https://golang.org/pkg/context/#WithTimeout.
//
// vals must be a []S, []*S, []I or []P, for some struct type S, some interface
// type I, or some non-interface non-pointer type P such that P or *P implements
// datastore.PropertyLoadSaver. If an []I, each element must be a valid dst for
// Get: it must be a struct pointer or implement datastore.PropertyLoadSaver.
//
// As a special case, datastore.PropertyList is an invalid type for dst, even
// though a PropertyList is a slice of structs. It is treated as invalid to
// avoid being mistakenly passed when []datastore.PropertyList was intended.
func (c *Client) GetMulti(ctx context.Context,
	keys []*datastore.Key, vals interface{}) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/qedus/nds.GetMulti")
	defer span.End()
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
			errs[i] = c.getMulti(ctx, keys, vals)
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
// Currently PropertyLoadSaver and KeyLoader is implemented. If there is no such entity
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
func (c *Client) Get(ctx context.Context, key *datastore.Key, val interface{}) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/qedus/nds.Get")
	defer span.End()
	// GetMulti catches nil interface; we need to catch nil ptr here.
	if val == nil {
		return datastore.ErrInvalidEntityType
	}

	keys := []*datastore.Key{key}
	vals := []interface{}{val}
	v := reflect.ValueOf(vals)
	if err := checkKeysValues(keys, v); err != nil {
		return err
	}

	err := c.getMulti(ctx, keys, v)
	if me, ok := err.(datastore.MultiError); ok {
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
	key      *datastore.Key
	cacheKey string

	val reflect.Value
	err error

	item *Item

	state cacheState
}

// getMulti attempts to get entities from the cache, then the datastore. It also
// tries to replenish the cache if needed available. It does this in such a way
// that GetMulti will never get stale results even if the function, datastore or
// server fails at any point. The caching strategy is borrowed from Python ndb
// with improvements that eliminate some consistency issues surrounding ndb,
// including http://goo.gl/3ByVlA.
func (c *Client) getMulti(ctx context.Context,
	keys []*datastore.Key, vals reflect.Value) error {

	if c.cacher != nil {
		num := len(keys)
		cacheItems := make([]cacheItem, num)
		for i, key := range keys {
			cacheItems[i].key = key
			cacheItems[i].cacheKey = createCacheKey(key)
			cacheItems[i].val = vals.Index(i)
			cacheItems[i].state = miss
		}

		c.loadCache(ctx, cacheItems)
		if err := cacheStatsByKind(ctx, cacheItems); err != nil {
			c.onError(ctx, errors.Wrapf(err, "nds:getMulti cacheStatsByKind"))
		}

		c.lockCache(ctx, cacheItems)

		if err := c.loadDatastore(ctx, cacheItems, vals.Type()); err != nil {
			return err
		}

		c.saveCache(ctx, cacheItems)

		me, errsNil := make(datastore.MultiError, len(cacheItems)), true
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
	return c.Client.GetMulti(ctx, keys, vals.Interface())
}

// loadCache will return the # of cache hits
func (c *Client) loadCache(ctx context.Context, cacheItems []cacheItem) {

	cacheKeys := make([]string, len(cacheItems))
	for i, cacheItem := range cacheItems {
		cacheKeys[i] = cacheItem.cacheKey
	}

	items, err := c.cacher.GetMulti(ctx, cacheKeys)
	if err != nil {
		for i := range cacheItems {
			cacheItems[i].state = externalLock
		}
		c.onError(ctx, errors.Wrapf(err, "nds:loadCache GetMulti"))
		return
	}

	for i, cacheKey := range cacheKeys {
		if item, ok := items[cacheKey]; ok {
			switch item.Flags {
			case lockItem:
				cacheItems[i].state = externalLock
			case noneItem:
				cacheItems[i].state = done
				cacheItems[i].err = datastore.ErrNoSuchEntity
			case entityItem:
				pl := datastore.PropertyList{}
				if err := unmarshal(item.Value, &pl); err != nil {
					c.onError(ctx, errors.Wrapf(err, "nds:loadCache unmarshal"))
					cacheItems[i].state = externalLock
					break
				}
				if err := setValue(cacheItems[i].val, pl, cacheItems[i].key); err == nil {
					cacheItems[i].state = done
				} else {
					c.onError(ctx, errors.Wrapf(err, "nds:loadCache setValue"))
					cacheItems[i].state = externalLock
				}
			default:
				c.onError(ctx, errors.Errorf("nds:loadCache unknown item.Flags %d", item.Flags))
				cacheItems[i].state = externalLock
			}
		}
	}
}

// itemLock creates a pseudorandom cache lock value that enables each call of
// Get/GetMulti to determine if a lock retrieved from the cache is the one it
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

func (c *Client) lockCache(ctx context.Context, cacheItems []cacheItem) {

	lockItems := make([]*Item, 0, len(cacheItems))
	lockCacheKeys := make([]string, 0, len(cacheItems))
	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {

			item := &Item{
				Key:        cacheItem.cacheKey,
				Flags:      lockItem,
				Value:      itemLock(),
				Expiration: cacheLockTime,
			}
			cacheItems[i].item = item
			lockItems = append(lockItems, item)
			lockCacheKeys = append(lockCacheKeys, cacheItem.cacheKey)
		}
	}

	if len(lockItems) > 0 {
		// We don't care if there are errors here.
		if err := c.cacher.AddMulti(ctx, lockItems); err != nil {
			c.onError(ctx, errors.Wrap(err, "nds:lockCache AddMulti"))
		}

		// Get the items again so we can use CAS when updating the cache.
		items, err := c.cacher.GetMulti(ctx, lockCacheKeys)

		// Cache failed so forget about it and just use the datastore.
		if err != nil {
			for i, cacheItem := range cacheItems {
				if cacheItem.state == miss {
					cacheItems[i].state = externalLock
				}
			}
			c.onError(ctx, errors.Wrap(err, "nds:lockCache GetMulti"))
			return
		}

		// Cache worked so figure out what items we got.
		for i, cacheItem := range cacheItems {
			if cacheItem.state == miss {
				if item, ok := items[cacheItem.cacheKey]; ok {
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
							c.onError(ctx, errors.Wrap(err, "nds:lockCache unmarshal"))
							cacheItems[i].state = externalLock
							break
						}
						if err := setValue(cacheItems[i].val, pl, cacheItems[i].key); err == nil {
							cacheItems[i].state = done
						} else {
							c.onError(ctx, errors.Wrap(err, "nds:lockCache setValue"))
							cacheItems[i].state = externalLock
						}
					default:
						c.onError(ctx, errors.Errorf("nds:lockCache unknown item.Flags %d",
							item.Flags))
						cacheItems[i].state = externalLock
					}
				} else {
					// We just added a cache item but it now isn't available so
					// treat it as an extarnal lock.
					cacheItems[i].state = externalLock
				}
			}
		}
	}
}

func (c *Client) loadDatastore(ctx context.Context, cacheItems []cacheItem,
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

	if getMultiHook != nil {
		if err := getMultiHook(ctx, keys, vals); err != nil {
			return err
		}
	}

	if len(keys) == 0 {
		return nil
	}

	var me datastore.MultiError
	if err := c.Client.GetMulti(ctx, keys, vals); err == nil {
		me = make(datastore.MultiError, len(keys))
	} else if e, ok := err.(datastore.MultiError); ok {
		me = e
	} else {
		return err
	}

	for i, index := range cacheItemsIndex {
		switch me[i] {
		case nil:
			pl := vals[i]
			val := cacheItems[index].val

			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = entityItem
				cacheItems[index].item.Expiration = 0
				if data, err := marshal(pl); err == nil {
					cacheItems[index].item.Value = data
				} else {
					cacheItems[index].state = externalLock
					c.onError(ctx, errors.Wrap(err, "nds:loadDatastore marshal"))
				}
			}

			if err := setValue(val, pl, cacheItems[index].key); err != nil {
				cacheItems[index].err = err
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

func (c *Client) saveCache(ctx context.Context, cacheItems []cacheItem) {
	saveItems := make([]*Item, 0, len(cacheItems))
	for _, cacheItem := range cacheItems {
		if cacheItem.state == internalLock {
			saveItems = append(saveItems, cacheItem.item)
		}
	}

	if len(saveItems) == 0 {
		return
	}

	if err := c.cacher.CompareAndSwapMulti(ctx, saveItems); err != nil {
		c.onError(ctx, errors.Wrap(err, "nds:saveCache CompareAndSwapMulti"))
	}
}
