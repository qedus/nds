package nds_test

import (
	"appengine/aetest"
	"github.com/qedus/nds"
	"testing"
)

func TestQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	parentKey := nds.NewKey(c, "Parent", "string", 0, nil)
	q := nds.NewQuery("Entity").Ancestor(parentKey)

	vals := []testEntity{}
	keys, err := q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 0 {
		t.Fatal("keys != 0")
	}

	if len(vals) != 0 {
		t.Fatal("vals != 0")
	}

	keys = []*nds.Key{}
	entities := []testEntity{}
	for i := 1; i < 6; i++ {
		keys = append(keys, nds.NewKey(c, "Entity", "", int64(i), parentKey))
		entities = append(entities, testEntity{i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Ancestor.
	vals = []testEntity{}
	q = nds.NewQuery("Entity").Ancestor(parentKey)
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count, err := q.Count(c)
	if err != nil {
		t.Fatal(err)
	}

	if count != 5 {
		t.Fatal("count != 5")
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}

	// Filter
	vals = []testEntity{}
	q = nds.NewQuery("Entity").Ancestor(parentKey)
	q = q.Filter("Val >", 2)
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 3 {
		t.Fatal("keys != 3")
	}

	if len(vals) != 3 {
		t.Fatal("vals != 3")
	}

}
