package nds_test

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/qedus/nds"

	"appengine"
	"appengine/aetest"
	"appengine/datastore"
)

func TestPutGetDelete(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	incompleteKey := datastore.NewIncompleteKey(c, "Entity", nil)
	key, err := nds.Put(c, incompleteKey, &testEntity{43})
	if err != nil {
		t.Fatal(err)
	}

	if key.Incomplete() {
		t.Fatal("Key is incomplete")
	}

	te := &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.Val != 43 {
		t.Fatal("te.Val != 43", te.Val)
	}

	// Get from cache.
	te = &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.Val != 43 {
		t.Fatal("te.Val != 43", te.Val)
	}

	// Change value.
	if _, err := nds.Put(c, key, &testEntity{64}); err != nil {
		t.Fatal(err)
	}

	// Get from cache.
	te = &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.Val != 64 {
		t.Fatal("te.Val != 64", te.Val)
	}

	if err := nds.Delete(c, key); err != nil {
		t.Fatal(err)
	}

	if err := nds.Get(c, key, &testEntity{}); err != datastore.ErrNoSuchEntity {
		t.Fatal("expected datastore.ErrNoSuchEntity")
	}
}

func TestInterfaces(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	incompleteKey := datastore.NewIncompleteKey(c, "Entity", nil)
	incompleteKeys := []*datastore.Key{incompleteKey}
	entities := []interface{}{&testEntity{43}}
	keys, err := nds.PutMulti(c, incompleteKeys, entities)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatal("len(keys) != 1")
	}

	if keys[0].Incomplete() {
		t.Fatal("Key is incomplete")
	}

	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		//if err := datastore.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 43 {
		t.Fatal("te.Val != 43")
	}

	// Get from cache.
	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 43 {
		t.Fatal("te.Val != 43")
	}

	// Change value.
	entities = []interface{}{&testEntity{64}}
	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore with struct.
	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 64 {
		t.Fatal("te.Val != 64")
	}

	if err := nds.DeleteMulti(c, keys); err != nil {
		t.Fatal(err)
	}

	entities = []interface{}{testEntity{}}
	err = nds.GetMulti(c, keys, entities)
	if me, ok := err.(appengine.MultiError); ok {

		if len(me) != 1 {
			t.Fatal("expected 1 appengine.MultiError")
		}
		if me[0] != datastore.ErrNoSuchEntity {
			t.Fatal("expected datastore.ErrNoSuchEntity")
		}
	} else {
		t.Fatal("expected datastore.ErrNoSuchEntity", err)
	}
}

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
		if _, err := nds.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		respEntities := []testEntity{}
		for _ = range keys {
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

	for _, count := range []int{999, 1000, 1001} {

		// Create entities.
		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := 0; i < count; i++ {
			key := datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil)
			keys = append(keys, key)
			entities = append(entities, testEntity{i})
		}

		// Save every other entity.
		putKeys := []*datastore.Key{}
		putEntities := []testEntity{}
		for i, key := range keys {
			if i%2 == 0 {
				putKeys = append(putKeys, key)
				putEntities = append(putEntities, entities[i])
			}
		}

		if _, err := nds.PutMulti(c, putKeys, putEntities); err != nil {
			t.Fatal(err)
		}

		respEntities := make([]testEntity, len(keys))
		err := nds.GetMulti(c, keys, respEntities)
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

	// Create entities.
	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := 0; i < entityCount; i++ {
		key := datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil)
		keys = append(keys, key)
		entities = append(entities, testEntity{i})
	}

	// Save every other entity.
	putKeys := []*datastore.Key{}
	putEntities := []testEntity{}
	for i, key := range keys {
		if i%2 == 0 {
			putKeys = append(putKeys, key)
			putEntities = append(putEntities, entities[i])
		}
	}
	if keys, err := nds.PutMulti(c, putKeys, putEntities); err != nil {
		t.Fatal(err)
	} else if len(keys) != len(putKeys) {
		t.Fatal("incorrect key len")
	}

	// Get from datastore.
	respEntities := make([]testEntity, len(keys))
	err = nds.GetMulti(c, keys, respEntities)
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
	err = nds.GetMulti(c, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok = err.(appengine.MultiError)
	if !ok {
		t.Fatalf("not an appengine.MultiError: %s", err)
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
	respEntities = make([]testEntity, len(keys))
	err = nds.GetMulti(c, keys, respEntities)
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

	key := datastore.NewKey(c, "Entity", "", 3, nil)
	keys := []*datastore.Key{key}
	entity := testEntity{42}
	entities := []testEntity{entity}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	err = nds.RunInTransaction(c, func(tc appengine.Context) error {
		entities := make([]testEntity, 1, 1)
		if err := nds.GetMulti(tc, keys, entities); err != nil {
			t.Fatal(err)
		}
		entity := entities[0]

		if entity.Val != 42 {
			t.Fatalf("entity.Val != 42: %d", entity.Val)
		}

		entities[0].Val = 43

		putKeys, err := nds.PutMulti(tc, keys, entities)
		if err != nil {
			t.Fatal(err)
		} else if len(putKeys) != 1 {
			t.Fatal("putKeys should be len 1")
		} else if !putKeys[0].Equal(key) {
			t.Fatal("keys not equal")
		}
		return nil

	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	entities = make([]testEntity, 1, 1)
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
	entity = entities[0]
	if entity.Val != 43 {
		t.Fatalf("entity.Val != 43: %d", entity.Val)
	}
}

func TestLoadSaveStruct(t *testing.T) {
	type testEntity struct {
		IntValue      int `datastore:",noindex"`
		StringValue   string
		MultipleValue []int64
	}

	te := testEntity{10, "ten", []int64{1, 2, 3}}
	pl := datastore.PropertyList{}
	if err := nds.SaveStruct(&te, &pl); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		value    interface{}
		noIndex  bool
		multiple bool
	}{
		{"IntValue", int64(10), true, false},
		{"StringValue", "ten", false, false},
		{"MultipleValue", int64(1), false, true},
		{"MultipleValue", int64(2), false, true},
		{"MultipleValue", int64(3), false, true},
	}

	for i, test := range tests {
		prop := pl[i]
		if prop.Name != test.name {
			t.Fatal("incorrect name")
		}
		if prop.Value != test.value {
			t.Fatalf("incorrect value required %+v got %+v",
				prop.Value, test.value)
		}
		if prop.NoIndex != test.noIndex {
			t.Fatal("incorrect no index")
		}
		if prop.Multiple != test.multiple {
			t.Fatal("incorrect multiple flag")
		}
	}

	loadTestEntity := testEntity{}
	if err := nds.LoadStruct(&loadTestEntity, &pl); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(te, loadTestEntity) {
		t.Fatal("entities not equal")
	}
}
