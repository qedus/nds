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
	for _, count := range []int{999, 1000, 1001} {

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
					t.Fatal("expecting datastore.ErrNoSuchEntity but got", e)
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

	for _, count := range []int{999, 1000, 1001} {

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
		for _ = range keys {
			respEntities = append(respEntities, testEntity{})
		}

		cc := nds.NewContext(c)
		if err := nds.GetMulti(cc, keys, respEntities); err != nil {
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

	for _, count := range []int{999, 1000, 1001} {

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

		cc := nds.NewContext(c)
		respEntities := make([]testEntity, len(keys))
		err := nds.GetMulti(cc, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		if me, ok := err.(appengine.MultiError); !ok {
			t.Fatal("not appengine.MultiError")
		} else if len(me) != len(keys) {
			t.Fatal("incorrect length appengine.MultiError")
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
				t.Fatalf("incorrect error, index %d", i)
			}
		}
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

	cc := nds.NewContext(c)

	// Create entities.
	keys := []*datastore.Key{}
	entities := []*testEntity{}
	for i := 0; i < entityCount; i++ {
		key := datastore.NewKey(cc, "Test", strconv.Itoa(i), 0, nil)
		keys = append(keys, key)
		entities = append(entities, &testEntity{i})
	}

	// Save every other entity.
	putKeys := []*datastore.Key{}
	putEntities := []*testEntity{}
	for i, key := range keys {
		if i%2 == 0 {
			putKeys = append(putKeys, key)
			putEntities = append(putEntities, entities[i])
		}
	}
	if keys, err := nds.PutMulti(cc, putKeys, putEntities); err != nil {
		t.Fatal(err)
	} else if len(keys) != len(putKeys) {
		t.Fatal("incorrect key len")
	}

	// Get from datastore.
	respEntities := make([]testEntity, len(keys))
	err = nds.GetMulti(cc, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok := err.(appengine.MultiError)
	if !ok {
		t.Fatalf("not an appengine.MultiError: %+T, %s", err, err)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatalf("should be nil error: %s", me[i])
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
	err = nds.GetMulti(cc, keys, respEntities)
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

	// Get from memcache.
	cc = nds.NewContext(c)
	respEntities = make([]testEntity, len(keys))
	err = nds.GetMulti(cc, keys, respEntities)
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

func TestRunInTransaction(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	entity := &testEntity{42}
	key := datastore.NewKey(c, "Entity", "", 3, nil)

	if _, err := datastore.Put(c, key, entity); err != nil {
		t.Fatal(err)
	}

	cc := nds.NewContext(c)
	err = nds.RunInTransaction(cc, func(tc appengine.Context) error {
		entity = &testEntity{}
		if err := nds.Get(tc, key, entity); err != nil {
			t.Fatal(err)
		}
		if entity.Val != 42 {
			t.Fatalf("entity.Val != 42: %d", entity.Val)
		}
		entity.Val = 43
		if putKey, err := nds.Put(tc, key, entity); err != nil {
			t.Fatal(err)
		} else if !putKey.Equal(key) {
			t.Fatal("keys not equal")
		}
		entity = &testEntity{}
		if err := nds.Get(tc, key, entity); err != nil {
			t.Fatal(err)
		}
		if entity.Val != 43 {
			t.Fatalf("entity.Val != 43: %d", entity.Val)
		}
		return nil

	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	entity = &testEntity{}
	if err := datastore.Get(c, key, entity); err != nil {
		t.Fatal(err)
	}
	if entity.Val != 43 {
		t.Fatalf("incorrect entity value: %d", entity.Val)
	}
}
