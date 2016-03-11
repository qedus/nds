package nds_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

func TestPutMulti(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type TestEntity struct {
		Value int
	}

	for _, count := range []int{499, 500, 501} {
		keys := make([]*datastore.Key, count)
		entities := make([]TestEntity, count)

		for i := range keys {
			keys[i] = datastore.NewKey(c, "TestEntity", strconv.Itoa(i), 0, nil)
			entities[i] = TestEntity{i}
		}

		if _, err := nds.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		entities = make([]TestEntity, count)
		if err := nds.GetMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPutMultiError(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	expectedErrs := appengine.MultiError{
		nil,
		errors.New("expected error"),
	}

	nds.SetDatastorePutMulti(func(c context.Context,
		keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
		return keys, expectedErrs
	})
	defer nds.SetDatastorePutMulti(datastore.PutMulti)

	keys := []*datastore.Key{
		datastore.NewKey(c, "Test", "", 1, nil),
		datastore.NewKey(c, "Test", "", 2, nil),
	}

	type TestEntity struct {
		Value int
	}
	entities := []TestEntity{
		{1},
		{2},
	}

	_, err := nds.PutMulti(c, keys, entities)
	me, ok := err.(appengine.MultiError)
	if !ok {
		t.Fatal("expected appengine.MultiError")
	}
	for i, e := range me {
		if e != expectedErrs[i] {
			t.Fatal("error incorrect")
		}
	}
}

func TestPutMultiNoPropertyList(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	pl := datastore.PropertyList{datastore.Property{}}

	if _, err := nds.PutMulti(c, keys, pl); err == nil {
		t.Fatal("expecting no PropertyList error")
	}
}

func TestPutPropertyLoadSaver(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	te := &testEntity{2}
	pl, err := datastore.SaveStruct(te)
	if err != nil {
		t.Fatal(err)
	}

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}

	pls := datastore.PropertyList(pl)
	if _, err := nds.PutMulti(c, keys,
		[]datastore.PropertyLoadSaver{&pls}); err != nil {
		t.Fatal(err)
	}

	getPl := datastore.PropertyList{}
	if err := nds.GetMulti(c,
		keys, []datastore.PropertyLoadSaver{&getPl}); err != nil {
		t.Fatal(err)
	}
	getTe := &testEntity{}
	if err := datastore.LoadStruct(getTe, getPl); err != nil {
		t.Fatal(err)
	}
	if te.IntVal != getTe.IntVal {
		t.Fatal("expected same IntVal", getTe.IntVal)
	}
}

func TestPutNilArgs(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	if _, err := nds.Put(c, nil, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestPutMultiLockFailure(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	nds.SetMemcacheSetMulti(func(c context.Context,
		items []*memcache.Item) error {
		return errors.New("expected error")
	})

	defer func() {
		nds.SetMemcacheSetMulti(memcache.SetMulti)
	}()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	vals := []testEntity{{42}}

	if _, err := nds.PutMulti(c, keys, vals); err == nil {
		t.Fatal("expected nds.PutMulti error")
	}
}

// Make sure PutMulti still works if we have a memcache unlock failure.
func TestPutMultiUnlockMemcacheSuccess(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	nds.SetMemcacheDeleteMulti(func(c context.Context, keys []string) error {
		return errors.New("expected error")
	})

	defer func() {
		nds.SetMemcacheDeleteMulti(memcache.DeleteMulti)
	}()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	vals := []testEntity{{42}}

	if _, err := nds.PutMulti(c, keys, vals); err != nil {
		t.Fatal(err)
	}
}

func TestPutDatastoreMultiError(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	expectedErr := errors.New("expected error")

	nds.SetDatastorePutMulti(func(c context.Context,
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

func TestPutMultiZeroKeys(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	if _, err := nds.PutMulti(c, []*datastore.Key{},
		[]interface{}{}); err != nil {
		t.Fatal(err)
	}
}
