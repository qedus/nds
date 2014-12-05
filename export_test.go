package nds

import (
	"reflect"

	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

var (
	PropertyLoadSaverToPropertyList = propertyLoadSaverToPropertyList

	ZeroMemcacheAddMulti            = zeroMemcacheAddMulti
	ZeroMemcacheCompareAndSwapMulti = zeroMemcacheCompareAndSwapMulti
	ZeroMemcacheDeleteMulti         = zeroMemcacheDeleteMulti
	ZeroMemcacheGetMulti            = zeroMemcacheGetMulti
	ZeroMemcacheSetMulti            = zeroMemcacheSetMulti

	MarshalPropertyList   = marshalPropertyList
	UnmarshalPropertyList = unmarshalPropertyList

	NoneItem   = noneItem
	EntityItem = entityItem
)

func SetMemcacheAddMulti(f func(c appengine.Context,
	items []*memcache.Item) error) {
	memcacheAddMulti = f
}

func SetMemcacheCompareAndSwapMulti(f func(c appengine.Context,
	items []*memcache.Item) error) {
	memcacheCompareAndSwapMulti = f
}

func SetMemcacheDeleteMulti(f func(c appengine.Context, keys []string) error) {
	memcacheDeleteMulti = f
}

func SetMemcacheGetMulti(f func(c appengine.Context,
	keys []string) (map[string]*memcache.Item, error)) {
	memcacheGetMulti = f
}

func SetMemcacheSetMulti(f func(c appengine.Context,
	items []*memcache.Item) error) {
	memcacheSetMulti = f
}

func SetDatastorePutMulti(f func(c appengine.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error)) {
	datastorePutMulti = f
}

func SetDatastoreGetMulti(f func(c appengine.Context,
	keys []*datastore.Key, vals interface{}) error) {
	datastoreGetMulti = f
}

func SetMarshal(f func(pl datastore.PropertyList) ([]byte, error)) {
	marshal = f
}

func SetUnmarshal(f func(data []byte, pl *datastore.PropertyList) error) {
	unmarshal = f
}

func SetValue(val reflect.Value, pl datastore.PropertyList) error {
	return setValue(val, pl)
}
