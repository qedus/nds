package nds

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"math/rand"
	"reflect"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

const (
	// memcachePrefix is the namespace memcache uses to store entities.
	memcachePrefix = "NDS1:"

	// memcacheLockTime is the maximum length of time a memcache lock will be
	// held for. 32 seconds is choosen as 30 seconds is the maximum amount of
	// time an underlying datastore call will retry even if the API reports a
	// success to the user.
	memcacheLockTime = 32 * time.Second

	// memcacheMaxKeySize is the maximum size a memcache item key can be. Keys
	// greater than this size are automatically hashed to a smaller size.
	memcacheMaxKeySize = 250
)

var (
	typeOfPropertyLoadSaver = reflect.TypeOf(
		(*datastore.PropertyLoadSaver)(nil)).Elem()
	typeOfPropertyList = reflect.TypeOf(datastore.PropertyList(nil))
)

// The variables in this block are here so that we can test all error code
// paths by substituting the respective functions with error producing ones.
var (
	datastoreDeleteMulti = datastore.DeleteMulti
	datastoreGetMulti    = datastore.GetMulti
	datastorePutMulti    = datastore.PutMulti

	memcacheAddMulti            = memcache.AddMulti
	memcacheCompareAndSwapMulti = memcache.CompareAndSwapMulti
	memcacheDeleteMulti         = memcache.DeleteMulti
	memcacheGetMulti            = memcache.GetMulti
	memcacheSetMulti            = memcache.SetMulti

	marshal   = marshalPropertyList
	unmarshal = unmarshalPropertyList
)

const (
	noneItem uint32 = iota
	entityItem
	lockItem
)

func init() {
	gob.Register(time.Time{})
	gob.Register(datastore.ByteString{})
	gob.Register(&datastore.Key{})
	gob.Register(appengine.BlobKey(""))
	gob.Register(appengine.GeoPoint{})
}

func itemLock() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rand.Uint32())
	return b
}

func checkMultiArgs(keys []*datastore.Key, v reflect.Value) error {
	if v.Kind() != reflect.Slice {
		return errors.New("nds: vals is not a slice")
	}

	if len(keys) != v.Len() {
		return errors.New("nds: keys and vals slices have different length")
	}

	isNilErr, nilErr := false, make(appengine.MultiError, len(keys))
	for i, key := range keys {
		if key == nil {
			isNilErr = true
			nilErr[i] = datastore.ErrInvalidKey
		}
	}
	if isNilErr {
		return nilErr
	}

	if v.Type() == typeOfPropertyList {
		return errors.New("nds: PropertyList not supported")
	}

	elemType := v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		return nil
	}

	switch elemType.Kind() {
	case reflect.Struct, reflect.Interface:
		return nil
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return nil
		}
	}
	return errors.New("nds: unsupported vals type")
}

func createMemcacheKey(key *datastore.Key) string {
	memcacheKey := memcachePrefix + key.Encode()
	if len(memcacheKey) > memcacheMaxKeySize {
		hash := sha1.Sum([]byte(memcacheKey))
		memcacheKey = hex.EncodeToString(hash[:])
	}
	return memcacheKey
}

func marshalPropertyList(pl datastore.PropertyList) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(&pl); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalPropertyList(data []byte, pl *datastore.PropertyList) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(pl)
}

func setValue(val reflect.Value, pl datastore.PropertyList) error {

	if reflect.PtrTo(val.Type()).Implements(typeOfPropertyLoadSaver) {
		val = val.Addr()
	}

	if pls, ok := val.Interface().(datastore.PropertyLoadSaver); ok {
		return pls.Load(pl)
	}

	if val.Kind() == reflect.Struct {
		val = val.Addr()
	}
	if val.Kind() == reflect.Ptr && val.Type().Elem().Kind() == reflect.Struct && val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}
	return datastore.LoadStruct(val.Interface(), pl)
}
