package nds_test

import (
	"testing"

	"github.com/qedus/nds"

	"errors"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

func TestDelete(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
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

	entities = make([]testEntity, 1)
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
	entity := entities[0]
	if entity.Val != 43 {
		t.Fatal("incorrect entity.Val", entity.Val)
	}

	if err := nds.DeleteMulti(c, keys); err != nil {
		t.Fatal(err)
	}

	keys = []*datastore.Key{key}
	entities = make([]testEntity, 1)
	err := nds.GetMulti(c, keys, entities)
	if me, ok := err.(appengine.MultiError); ok {
		if me[0] != datastore.ErrNoSuchEntity {
			t.Fatal("entity should be deleted", entities)
		}
	} else {
		t.Fatal("expected appengine.MultiError")
	}
}

func TestDeleteNilKey(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

	if err := nds.Delete(c, nil); err != datastore.ErrInvalidKey {
		t.Fatal("expected nil key error")
	}
}

func TestDeleteIncompleteKey(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

	if err := nds.Delete(c, nil); err != datastore.ErrInvalidKey {
		t.Fatal("expected invalid key error")
	}
}

func TestDeleteMemcacheFail(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
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
	c, closeFunc := NewContext(t, nil)
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
