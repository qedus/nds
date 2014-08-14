package nds_test

import (
	"errors"
	"testing"

	"appengine"
	"appengine/memcache"

	"github.com/qedus/nds"

	"appengine/aetest"
	"appengine/datastore"
)

func TestPutMultiNoPropertyList(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	pl := datastore.PropertyList{datastore.Property{}}

	if _, err := nds.PutMulti(c, keys, pl); err == nil {
		t.Fatal("expecting no PropertyList error")
	}
}

func TestPutNilArgs(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := nds.Put(c, nil, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestPutMultiLockFailure(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int
	}

	nds.SetMemcacheSetMulti(func(c appengine.Context,
		items []*memcache.Item) error {
		return errors.New("expected error")
	})

	defer func() {
		nds.SetMemcacheSetMulti(memcache.SetMulti)
	}()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	vals := []testEntity{testEntity{42}}

	if _, err := nds.PutMulti(c, keys, vals); err == nil {
		t.Fatal("expected nds.PutMulti error")
	}
}

// Make sure PutMulti still works if we have a memcache unlock failure.
func TestPutMultiUnlockMemcacheSuccess(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int
	}

	nds.SetMemcacheDeleteMulti(func(c appengine.Context, keys []string) error {
		return errors.New("expected error")
	})

	defer func() {
		nds.SetMemcacheDeleteMulti(memcache.DeleteMulti)
	}()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	vals := []testEntity{testEntity{42}}

	if _, err := nds.PutMulti(c, keys, vals); err != nil {
		t.Fatal(err)
	}
}
