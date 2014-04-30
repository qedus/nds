package nds

import (
	"appengine"
	"appengine/datastore"
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
	"sync"
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

	// memcacheLock is the value that is used to lock memcache.
	memcacheLock = uint32(1)
)

func checkMultiArgs(keys []*datastore.Key, v reflect.Value) error {
	if v.Kind() != reflect.Slice {
		return errors.New("nds: dst is not a slice")
	}

	if len(keys) != v.Len() {
		return errors.New("nds: key and dst slices have different length")
	}

	return nil
}

// NewContext returns an appengine.Context that allows this package to use
// use memory cache and memcache when operation on the datastore.
func NewContext(c appengine.Context) appengine.Context {
	return &context{
		Context: c,
		RWMutex: &sync.RWMutex{},
		cache:   map[string]datastore.PropertyList{},
	}
}

type context struct {
	appengine.Context

	// RWMutex is used to protect cache during concurrent access. It needs to
	// be a pointer so it can be copied between transactional and
	// non-transactional contexts when we copy the cache map.
	*sync.RWMutex

	// cache is the memory cache for entities. This could probably be changed
	// to map[string]interface{} in future versions so we don't rely on
	// datastore.PropertyList.
	// The string key is the datastore.Key.Encode() value.
	cache map[string]datastore.PropertyList

	// inTransaction is used to notify our GetMulti, PutMulti and DeleteMulti
	// functions that we are in a transaction as their memory and memcache
	// sync mechanisims change subtly.
	inTransaction bool
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
