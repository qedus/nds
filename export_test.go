package nds

import (
	"reflect"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

var (
	MarshalPropertyList   = marshalPropertyList
	UnmarshalPropertyList = unmarshalPropertyList

	NoneItem   = noneItem
	EntityItem = entityItem

	MemcacheMaxKeySize = memcacheMaxKeySize
)

func SetMemcacheAddMulti(f func(c context.Context,
	items []*memcache.Item) error) {
	memcacheAddMulti = f
}

func SetMemcacheCompareAndSwapMulti(f func(c context.Context,
	items []*memcache.Item) error) {
	memcacheCompareAndSwapMulti = f
}

func SetMemcacheDeleteMulti(f func(c context.Context, keys []string) error) {
	memcacheDeleteMulti = f
}

func SetMemcacheGetMulti(f func(c context.Context,
	keys []string) (map[string]*memcache.Item, error)) {
	memcacheGetMulti = f
}

func SetMemcacheSetMulti(f func(c context.Context,
	items []*memcache.Item) error) {
	memcacheSetMulti = f
}

func SetDatastorePutMulti(f func(c context.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error)) {
	datastorePutMulti = f
}

func SetDatastoreGetMulti(f func(c context.Context,
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

func CreateMemcacheKey(key *datastore.Key) string {
	return createMemcacheKey(key)
}

func SetMemcacheNamespace(namespace string) {
	memcacheNamespace = namespace
}
