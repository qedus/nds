package nds

import (
	"appengine"
	"appengine/memcache"
)

var (
	MemcacheAddMulti            = memcacheAddMulti
	MemcacheCompareAndSwapMulti = memcacheCompareAndSwapMulti
	MemcacheDeleteMulti         = memcacheDeleteMulti
	MemcacheGetMulti            = memcacheGetMulti
	MemcacheSetMulti            = memcacheSetMulti

	DatastoreGetMulti    = datastoreGetMulti
	DatastoreDeleteMulti = datastoreDeleteMulti
	DatastorePutMulti    = datastorePutMulti
)

func SetMemcacheSetMulti(f func(c appengine.Context,
	items []*memcache.Item) error) {
	memcacheSetMulti = f
}
