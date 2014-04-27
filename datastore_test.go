package nds_test

import (
	"appengine"
	"appengine/aetest"
	"appengine/datastore"
	"github.com/qedus/nds"
	"strconv"
	"testing"
)

func TestGetMultiNoSuchEntity(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	// Test no such entity.
	for _, count := range []int{999, 1000, 1001, 5000, 5001} {

		keys := []*datastore.Key{}
		entities := []*testEntity{}
		for i := 0; i < count; i++ {
			keys = append(keys,
				datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil))
			entities = append(entities, &testEntity{})
		}

		err := nds.GetMulti(c, keys, entities)
		if me, ok := err.(appengine.MultiError); ok {
			if len(me) != count {
				t.Fatal("multi error length incorrect")
			}
			for _, e := range me {
				if e != datastore.ErrNoSuchEntity {
					t.Fatal(e)
				}
			}
		}
	}
}

func TestGetMultiNoErrors(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	for _, count := range []int{999, 1000, 1001, 5000, 5001} {

		// Create entities.
		keys := []*datastore.Key{}
		entities := []*testEntity{}
		for i := 0; i < count; i++ {
			key := datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil)
			keys = append(keys, key)
			entities = append(entities, &testEntity{i})
		}

		// Save entities.
		for i, key := range keys {
			if _, err := datastore.Put(c, key, entities[i]); err != nil {
				t.Fatal(err)
			}
		}

		respEntities := []testEntity{}
		for _, _ = range keys {
			respEntities = append(respEntities, testEntity{})
		}

		if err := nds.GetMulti(c, keys, respEntities); err != nil {
			t.Fatal(err)
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
		}
	}
}

func TestGetMultiErrorMix(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	for _, count := range []int{999, 1000, 1001, 5000, 5001} {

		// Create entities.
		keys := []*datastore.Key{}
		entities := []*testEntity{}
		for i := 0; i < count; i++ {
			key := datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil)
			keys = append(keys, key)
			entities = append(entities, &testEntity{i})
		}

		// Save every other entity.
		for i, key := range keys {
			if i%2 == 0 {
				if _, err := datastore.Put(c, key, entities[i]); err != nil {
					t.Fatal(err)
				}
			}
		}

		respEntities := make([]testEntity, len(keys))
		err := nds.GetMulti(c, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if i%2 == 0 {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
			} else if me, ok := err.(appengine.MultiError); ok {
				if me[i] != datastore.ErrNoSuchEntity {
					t.Fatalf("incorrect error %+v, index %d, of %d",
						me, i, count)
				}
			} else {
				t.Fatal("incorrect error, index %d", i)
			}
		}
	}
}

func TestLoadSaveStruct(t *testing.T) {
	type Test struct {
		Value string
	}
	saveEntity := &Test{Value: "one"}
	pl := datastore.PropertyList{}
	if err := nds.SaveStruct(saveEntity, &pl); err != nil {
		t.Fatal(err)
	}
	if len(pl) != 1 {
		t.Fatal("incorrect pl size")
	}

	loadEntity := &Test{}
	if err := nds.LoadStruct(loadEntity, &pl); err != nil {
		t.Fatal(err)
	}
	if loadEntity.Value != "one" {
		t.Fatal("incorrect loadEntity.Value")
	}
}

func TestMultiCache(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}
	const entityCount = 88

	cc := nds.NewCacheContext(c)

	// Create entities.
	keys := []*datastore.Key{}
	entities := []*testEntity{}
	for i := 0; i < entityCount; i++ {
		key := datastore.NewKey(cc, "Test", strconv.Itoa(i), 0, nil)
		keys = append(keys, key)
		entities = append(entities, &testEntity{i})
	}

	// Save every other entity.
	for i, key := range keys {
		if i%2 == 0 {
			if _, err := datastore.Put(c, key, entities[i]); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Get from datastore.
	respEntities := make([]testEntity, len(keys))
	err = nds.GetMultiCache(cc, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok := err.(appengine.MultiError)
	if !ok {
		t.Fatalf("not an appengine.MultiError: %+T", me)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatal("should be nil error")
			}
		} else {
			if re.Val != 0 {
				t.Fatal("entity not zeroed")
			}
			if me[i] != datastore.ErrNoSuchEntity {
				t.Fatalf("incorrect error %+v, index %d, of %d",
					me, i, entityCount)
			}
		}
	}

	// Get from local cache.
	respEntities = make([]testEntity, len(keys))
	err = nds.GetMultiCache(cc, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok = err.(appengine.MultiError)
	if !ok {
		t.Fatalf("not an appengine.MultiError: %+T", me)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatal("should be nil error")
			}
		} else {
			if re.Val != 0 {
				t.Fatal("entity not zeroed")
			}
			if me[i] != datastore.ErrNoSuchEntity {
				t.Fatalf("incorrect error %+v, index %d, of %d",
					me, i, entityCount)
			}
		}
	}
}
