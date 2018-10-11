package nds

import (
	"context"
	"reflect"

	"cloud.google.com/go/datastore"
)

var (
	MarshalPropertyList   = marshalPropertyList
	UnmarshalPropertyList = unmarshalPropertyList

	NoneItem   = noneItem
	EntityItem = entityItem

	CacheMaxKeySize = cacheMaxKeySize
)

func SetMarshal(f func(pl datastore.PropertyList) ([]byte, error)) {
	marshal = f
}

func SetUnmarshal(f func(data []byte, pl *datastore.PropertyList) error) {
	unmarshal = f
}

func SetValue(val reflect.Value, pl datastore.PropertyList, key *datastore.Key) error {
	return setValue(val, pl, key)
}

func CreateCacheKey(key *datastore.Key) string {
	return createCacheKey(key)
}

func SetDatastorePutMultiHook(f func() error) {
	putMultiHook = f
}

func SetDatastoreGetMultiHook(f func(c context.Context, keys []*datastore.Key, vals interface{}) error) {
	getMultiHook = f
}
