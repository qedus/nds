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

func SaveStruct(src interface{}, pl *datastore.PropertyList) error {
	return saveStruct(src, pl)
}

func LoadStruct(dst interface{}, pl datastore.PropertyList) error {
	return loadStruct(dst, pl)
}

func MarshalPropertyList(pl datastore.PropertyList) ([]byte, error) {
	return marshalPropertyList(pl)
}

func UnmarshalPropertyList(data []byte, pl *datastore.PropertyList) error {
	return unmarshalPropertyList(data, pl)
}

func SetValue(val reflect.Value, pl datastore.PropertyList) error {
	return setValue(val, pl)
}
