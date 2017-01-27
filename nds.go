package nds

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

const (
	// memcachePrefix is the namespace memcache uses to store entities.
	memcachePrefix = "NDS1:"

	// memcacheLockTime is the maximum length of time a memcache lock will be
	// held for. 32 seconds is chosen as 30 seconds is the maximum amount of
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
// paths by substituting them with error producing ones.
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

	// memcacheNamespace is the namespace where all memcached entities are
	// stored.
	memcacheNamespace = ""
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

type valueType int

const (
	valueTypeInvalid valueType = iota
	valueTypePropertyLoadSaver
	valueTypeStruct
	valueTypeStructPtr
	valueTypeInterface
)

func checkValueType(valType reflect.Type) valueType {

	if reflect.PtrTo(valType).Implements(typeOfPropertyLoadSaver) {
		return valueTypePropertyLoadSaver
	}

	switch valType.Kind() {
	case reflect.Struct:
		return valueTypeStruct
	case reflect.Interface:
		return valueTypeInterface
	case reflect.Ptr:
		valType = valType.Elem()
		if valType.Kind() == reflect.Struct {
			return valueTypeStructPtr
		}
	}
	return valueTypeInvalid
}

func checkKeysValues(keys []*datastore.Key, values reflect.Value) error {
	if values.Kind() != reflect.Slice {
		return errors.New("nds: valus is not a slice")
	}

	if len(keys) != values.Len() {
		return errors.New("nds: keys and values slices have different length")
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

	if values.Type() == typeOfPropertyList {
		return errors.New("nds: PropertyList not supported")
	}

	if ty := checkValueType(values.Type().Elem()); ty == valueTypeInvalid {
		return errors.New("nds: unsupported vals type")
	}
	return nil
}

func createMemcacheKey(key *datastore.Key) string {
	memcacheKey := memcachePrefix + key.Encode()
	if len(memcacheKey) > memcacheMaxKeySize {
		hash := sha1.Sum([]byte(memcacheKey))
		memcacheKey = hex.EncodeToString(hash[:])
	}
	return memcacheKey
}

func memcacheContext(c context.Context) (context.Context, error) {
	return appengine.Namespace(c, memcacheNamespace)
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

	valType := checkValueType(val.Type())

	if valType == valueTypePropertyLoadSaver || valType == valueTypeStruct {
		val = val.Addr()
	}

	if valType == valueTypeStructPtr && val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}

	if pls, ok := val.Interface().(datastore.PropertyLoadSaver); ok {
		return pls.Load(pl)
	}

	return datastore.LoadStruct(val.Interface(), pl)
}

func isErrorsNil(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return false
		}
	}
	return true
}

func groupErrors(errs []error, total, limit int) error {
	groupedErrs := make(appengine.MultiError, total)
	for i, err := range errs {
		lo := i * limit
		hi := (i + 1) * limit
		if hi > total {
			hi = total
		}
		if me, ok := err.(appengine.MultiError); ok {
			copy(groupedErrs[lo:hi], me)
		} else if err != nil {
			for j := lo; j < hi; j++ {
				groupedErrs[j] = err
			}
		}
	}
	return groupedErrs
}
