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

func TestPutPropertyLoadSaver(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int
	}

	te := &testEntity{2}
	pl := &datastore.PropertyList{}
	if err := nds.SaveStruct(te, pl); err != nil {
		t.Fatal(err)
	}

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}

	if _, err := nds.PutMulti(c,
		keys, []datastore.PropertyLoadSaver{pl}); err != nil {
		t.Fatal(err)
	}

	getPl := datastore.PropertyList{}
	if err := nds.GetMulti(c,
		keys, []datastore.PropertyLoadSaver{&getPl}); err != nil {
		t.Fatal(err)
	}
	getTe := &testEntity{}
	if err := nds.LoadStruct(getTe, getPl); err != nil {
		t.Fatal(err)
	}
	if te.IntVal != getTe.IntVal {
		t.Fatal("expected same IntVal", getTe.IntVal)
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

func TestPutDatastoreMultiError(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int
	}

	expectedErr := errors.New("expected error")

	nds.SetDatastorePutMulti(func(c appengine.Context,
		keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
		return nil, appengine.MultiError{expectedErr}
	})

	defer func() {
		nds.SetDatastorePutMulti(datastore.PutMulti)
	}()

	key := datastore.NewKey(c, "Test", "", 1, nil)
	val := &testEntity{42}

	if _, err := nds.Put(c, key, val); err == nil {
		t.Fatal("expected error")
	} else if err != expectedErr {
		t.Fatal("should be expectedErr")
	}
}
