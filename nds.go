package nds

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"time"
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
	// memcacheLock is the value that is used to lock memcache.
	memcacheLock = []byte{0}

	typeOfPropertyLoadSaver = reflect.TypeOf(
		(*datastore.PropertyLoadSaver)(nil)).Elem()
	typeOfPropertyList = reflect.TypeOf(datastore.PropertyList(nil))
)

func isItemLocked(item *memcache.Item) bool {
	return bytes.Equal(item.Value, memcacheLock)
}

func checkArgs(keys []*datastore.Key, v reflect.Value) error {
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
	case reflect.Struct:
		return nil
	case reflect.Interface:
		return nil
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return nil
		}
	}

	fmt.Println("Post:", elemType)
	return errors.New("nds: vals must be a slice of pointers")
}

type txContext struct {
	appengine.Context
}

func inTransaction(c appengine.Context) bool {
	_, ok := c.(txContext)
	return ok
}

func addrValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Struct {
		return v.Addr()
	}
	return v
}

func setValue(index int, vals reflect.Value, pl *datastore.PropertyList) error {
	elem := addrValue(vals.Index(index))
	return loadStruct(elem.Interface(), pl)
}

func decodePropertyList(data []byte) (datastore.PropertyList, error) {
	pl := datastore.PropertyList{}
	return pl, gob.NewDecoder(bytes.NewBuffer(data)).Decode(&pl)
}

func encodePropertyList(pl datastore.PropertyList) ([]byte, error) {
	b := &bytes.Buffer{}
	err := gob.NewEncoder(b).Encode(pl)
	return b.Bytes(), err
}

func createMemcacheKey(key *datastore.Key) string {
	return memcachePrefix + key.Encode()
}

// saveStruct saves src to a datastore.PropertyList.
func saveStruct(src interface{}, pl *datastore.PropertyList) error {
	c, err := make(chan datastore.Property), make(chan error)
	go func() {
		err <- datastore.SaveStruct(src, c)
	}()
	for p := range c {
		*pl = append(*pl, p)
	}
	return <-err
}

// loadStruct loads a datastore.PropertyList into dst.
func loadStruct(dst interface{}, pl *datastore.PropertyList) error {
	c := make(chan datastore.Property)
	go func() {
		for _, p := range *pl {
			c <- p
		}
		close(c)
	}()
	return datastore.LoadStruct(dst, c)
}
