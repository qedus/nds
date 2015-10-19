package nds_test

import (
	"strconv"
	"testing"

	"github.com/qedus/nds"

	"errors"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

func TestDeleteMulti(t *testing.T) {
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

		// Prime cache.
		entities = make([]TestEntity, count)
		if err := nds.GetMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		if err := nds.DeleteMulti(c, keys); err != nil {
			t.Fatal(err)
		}

		err := nds.GetMulti(c, keys, make([]TestEntity, count))
		if err == nil {
			t.Fatal("expect error")
		}

		me, ok := err.(appengine.MultiError)
		if !ok {
			t.Fatal("should be MultiError")
		}

		for _, e := range me {
			if e != datastore.ErrNoSuchEntity {
				t.Fatal("expected ErrNoSuchEntity")
			}
		}
	}
}

func TestDeleteNilKey(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	if err := nds.Delete(c, nil); err != datastore.ErrInvalidKey {
		t.Fatal("expected nil key error")
	}
}

func TestDeleteIncompleteKey(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	if err := nds.Delete(c, nil); err != datastore.ErrInvalidKey {
		t.Fatal("expected invalid key error")
	}
}

func TestDeleteMemcacheFail(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	key := datastore.NewKey(c, "Entity", "", 1, nil)
	keys := []*datastore.Key{key}
	entities := make([]testEntity, 1)
	entities[0].Val = 43

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	nds.SetMemcacheSetMulti(func(c context.Context,
		items []*memcache.Item) error {
		return errors.New("expected error")
	})

	defer func() {
		nds.SetMemcacheSetMulti(memcache.SetMulti)
	}()

	if err := nds.DeleteMulti(c, keys); err == nil {
		t.Fatal("expected DeleteMulti error")
	}
}

func TestDeleteInTransaction(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	key := datastore.NewKey(c, "TestEntity", "", 1, nil)
	if _, err := nds.Put(c, key, &testEntity{2}); err != nil {
		t.Fatal(err)
	}

	// Prime cache.
	if err := nds.Get(c, key, &testEntity{}); err != nil {
		t.Fatal(err)
	}

	if err := nds.RunInTransaction(c, func(tc context.Context) error {
		return nds.DeleteMulti(tc, []*datastore.Key{key})
	}, nil); err != nil {
		t.Fatal(err)
	}

	if err := nds.Get(c, key, &testEntity{}); err == nil {
		t.Fatal("expected no entity")
	} else if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}
