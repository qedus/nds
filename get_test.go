package nds_test

import (
	"testing"

	"github.com/qedus/nds"

	"appengine/aetest"
	"appengine/datastore"
)

func TestGetMultiStruct(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, testEntity{i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore.
	response := make([]testEntity, len(keys))
	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}

	// Get from cache.
	response = make([]testEntity, len(keys))
	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}
}

func TestGetMultiPtr(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, testEntity{i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore.
	response := make([]*testEntity, len(keys))
	for i := 0; i < len(response); i++ {
		response[i] = &testEntity{}
	}

	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}

	// Get from cache.
	response = make([]*testEntity, len(keys))
	for i := 0; i < len(response); i++ {
		response[i] = &testEntity{}
	}
	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}
}

func TestGetMultiInterface(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, testEntity{i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore.
	response := make([]interface{}, len(keys))
	for i := 0; i < len(response); i++ {
		response[i] = &testEntity{}
	}

	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if te, ok := response[i].(*testEntity); ok {
			if te.IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		} else {
			t.Fatal("incorrect type")
		}
	}

	// Get from cache.
	response = make([]interface{}, len(keys))
	for i := 0; i < len(response); i++ {
		response[i] = &testEntity{}
	}
	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if te, ok := response[i].(*testEntity); ok {
			if te.IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		} else {
			t.Fatal("incorrect type")
		}
	}
}

func TestGetMultiNoKeys(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}

	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
}

// datastore.PropertyLoadSaver is not supported to test it does not work.
func TestGetMultiPropertyLoadSaver(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := []*datastore.Key{}
	entities := []datastore.PropertyList{}

	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, datastore.PropertyList{})
	}
	if err := nds.GetMulti(c, keys, entities); err == nil {
		t.Fatal("expecting error")
	} else {
		t.Log(err)
	}
}

func TestGetMultiInterfaceError(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, testEntity{i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore.
	// No errors expected.
	response := []interface{}{&testEntity{}, &testEntity{}}

	if err := datastore.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if te, ok := response[i].(*testEntity); ok {
			if te.IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		} else {
			t.Fatal("incorrect type")
		}
	}

	// Get from cache.
	// Errors expected.
	response = []interface{}{&testEntity{}, testEntity{}}
	if err := datastore.GetMulti(c, keys, response); err == nil {
		t.Fatal("expected invalid entity type error")
	}
}
