package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

func SetMemcacheDeleteMulti(f func(c appengine.Context, keys []string) error) {
	memcacheDeleteMulti = f
}

func SetMemcacheSetMulti(f func(c appengine.Context,
	items []*memcache.Item) error) {
	memcacheSetMulti = f
}

func SetDatastorePutMulti(f func(c appengine.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error)) {
	datastorePutMulti = f
}
