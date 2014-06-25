package nds_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/qedus/nds"
	"testing"
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

    if _, err := nds.Put(c, key, &testEntity{43}); err != nil {
        t.Fatal(err)
    }

    entity := &testEntity{}
    if err := nds.Get(c, key, entity); err != nil {
        t.Fatal(err)
    }
    if entity.Val != 43 {
        t.Fatal("incorrect entity.Val", entity.Val)
    }

    if err := nds.Delete(c, key); err != nil {
        t.Fatal(err)
    }

    if err := nds.Get(c, key, &testEntity{}); err != datastore.ErrNoSuchEntity {
        t.Fatal("entity should be deleted")
    }
}
