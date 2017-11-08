package nds

import (
	"golang.org/x/net/context"
	"github.com/bradfitz/gomemcache/memcache"
	"cloud.google.com/go/datastore"
)

var mcClient *memcache.Client

func initMemCache(addr string) {
	mcClient = memcache.New(addr)
}
func memcacheAddMulti(c context.Context, items []*memcache.Item) error {
	if mcClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mcClient.Add(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}
func memcacheSetMulti(c context.Context, items []*memcache.Item) error {
	if mcClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mcClient.Set(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

func memcacheGetMulti(c context.Context, keys []string) (map[string]*memcache.Item, error) {
	if mcClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	items := make(map[string]*memcache.Item, len(keys))
	multiErr, any := make(datastore.MultiError, len(keys)), false
	for i, key := range keys {
		item, err := mcClient.Get(key)
		if err != nil {
			if err == memcache.ErrCacheMiss {
				continue
			}
			multiErr[i] = err
			any = true
			continue
		}
		items[key] = item
	}
	if any {
		return nil, multiErr
	}
	return items, nil
}

func memcacheDeleteMulti(c context.Context, keys []string) error {
	if mcClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(keys)), false
	for i, key := range keys {
		if err := mcClient.Delete(key); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

func memcacheCompareAndSwapMulti(c context.Context, items []*memcache.Item) error {
	if mcClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mcClient.CompareAndSwap(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

