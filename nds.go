package nds

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"time"

	"appengine/datastore"
	"appengine/memcache"
)

const (
	// memcachePrefix is the namespace memcache uses to store entities.
	memcachePrefix = "NDS0:"

	// memcacheLockTime is the maximum length of time a memcache lock will be
	// held for. 32 seconds is choosen as 30 seconds is the maximum amount of
	// time an underlying datastore call will retry even if the API reports a
	// success to the user.
	memcacheLockTime = 32 * time.Second
)

var (
	typeOfPropertyLoadSaver = reflect.TypeOf(
		(*datastore.PropertyLoadSaver)(nil)).Elem()
	typeOfPropertyList = reflect.TypeOf(datastore.PropertyList(nil))

	memcacheAddMulti            = memcache.AddMulti
	memcacheCompareAndSwapMulti = memcache.CompareAndSwapMulti
	memcacheGetMulti            = memcache.GetMulti
	memcacheDeleteMulti         = memcache.DeleteMulti
	memcacheSetMulti            = memcache.SetMulti

	datastoreDeleteMulti = datastore.DeleteMulti
	datastoreGetMulti    = datastore.GetMulti
	datastorePutMulti    = datastore.PutMulti

	// ErrInvalidKey is returned when an invalid key is presented.
	ErrInvalidKey = datastore.ErrInvalidKey

	// ErrNoSuchEntity is returned when no entity was found for a given key.
	ErrNoSuchEntity = datastore.ErrNoSuchEntity
)

const (
	noneItem uint32 = iota
	entityItem
	lockItem
)

func itemLock() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rand.Uint32())
	return b
}

func checkArgs(key *datastore.Key, val interface{}) error {
	if key == nil {
		return errors.New("nds: key is nil")
	}

	if val == nil {
		return errors.New("nds: val is nil")
	}

	v := reflect.ValueOf(val)
	if v.Type() == typeOfPropertyList {
		return errors.New("nds: PropertyList not supported")
	}

	switch v.Kind() {
	case reflect.Ptr:
		return nil
	default:
		return errors.New("nds: val must be a slice or pointer")
	}

}

func checkMultiArgs(keys []*datastore.Key, v reflect.Value) error {
	if v.Kind() != reflect.Slice {
		return errors.New("nds: vals is not a slice")
	}

	if len(keys) != v.Len() {
		return errors.New("nds: keys and vals slices have different length")
	}

	if v.Type() == typeOfPropertyList {
		return errors.New("nds: PropertyList not supported")
	}

	elemType := v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		return errors.New("nds: PropertyLoadSaver not supporded")
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
	return memcachePrefix + key.Encode()
}

// SaveStruct saves src to a datastore.PropertyList.
func SaveStruct(src interface{}, pl *datastore.PropertyList) error {
	c, err := make(chan datastore.Property), make(chan error)
	go func() {
		err <- datastore.SaveStruct(src, c)
	}()
	for p := range c {
		*pl = append(*pl, p)
	}
	return <-err
}

// LoadStruct loads a datastore.PropertyList into dst.
func LoadStruct(dst interface{}, pl *datastore.PropertyList) error {
	c := make(chan datastore.Property)
	go func() {
		for _, p := range *pl {
			c <- p
		}
		close(c)
	}()
	return datastore.LoadStruct(dst, c)
}
