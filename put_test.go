package nds_test

import (
	"testing"

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
