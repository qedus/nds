package nds_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/qedus/nds"
	"testing"
	"time"
)

func TestTimeout(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cc := nds.NewContext(c)
	tc := nds.Timeout(cc, 10*time.Second)

	key := datastore.NewIncompleteKey(cc, "TestEntity", nil)
	type TestEntity struct {
		Value int
	}
	key, err = nds.Put(tc, key, &TestEntity{52})
	if err != nil {
		t.Fatal(err)
	}

	if key.Incomplete() {
		t.Fatal("incomplete key")
	}

	// Create a new context to cause a memcache hit.
	entity := &TestEntity{}
	if err := nds.Get(nds.NewContext(c), key, entity); err != nil {
		t.Fatal(err)
	}
	if entity.Value != 52 {
		t.Fatal("incorrect entity.Value")
	}

	// Check we used caching by seeing if there is the entity in cache.
	stats, err := memcache.Stats(c)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Items == 0 {
		t.Fatal("nothing in cache")
	}
}
