package nds_test

import (
	"testing"

	"github.com/qedus/nds"

	"appengine"
	"appengine/aetest"
	"appengine/datastore"
)

func TestDelete(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

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
	err = nds.GetMulti(c, keys, entities)
	if me, ok := err.(appengine.MultiError); ok {
		if me[0] != nds.ErrNoSuchEntity {
			t.Fatal("entity should be deleted", entities)
		}
	} else {
		t.Fatal("expected appengine.MultiError")
	}
}

func TestDeleteNilKey(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := nds.Delete(c, nil); err != nds.ErrInvalidKey {
		t.Fatal("expected nil key error")
	}
}

func TestDeleteIncompleteKey(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := nds.Delete(c, nil); err != nds.ErrInvalidKey {
		t.Fatal("expected invalid key error")
	}
}
