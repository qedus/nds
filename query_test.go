package nds_test

import (
	"appengine"
	"appengine/aetest"
	"github.com/qedus/nds"
	"strconv"
	"testing"
)

type queryTestEntity struct {
	Val       int64
	StringVal string
}

func addQueryTestEntities(c appengine.Context,
	ancestorKey *nds.Key) ([]*nds.Key, error) {

	keys := []*nds.Key{}
	entities := []queryTestEntity{}
	for i := 1; i < 6; i++ {
		keys = append(keys, nds.NewKey(c, "Entity", "", int64(i), ancestorKey))
		entities = append(entities, queryTestEntity{int64(i), strconv.Itoa(i)})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		return nil, err
	}
	// To make sure the entities are added to the index.
	if err := nds.GetMulti(c, keys, entities); err != nil {
		return nil, err
	}
	return keys, nil
}

func TestGetAllQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	q := nds.NewQuery("Entity")

	vals := []queryTestEntity{}
	keys, err := q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure no values.
	if len(keys) != 0 {
		t.Fatal("keys != 0")
	}

	if len(vals) != 0 {
		t.Fatal("vals != 0")
	}

	keys, err = addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	vals = []queryTestEntity{}
	q = nds.NewQuery("Entity")
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count, err := q.Count(c)
	if err != nil {
		t.Fatal(err)
	}

	if count != 5 {
		t.Fatal("count != 5:", count)
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}

	for _, val := range vals {
		if val.Val < 1 || val.Val > 5 {
			t.Fatal("val outside range")
		}

		if val.StringVal == "" {
			t.Fatal("val.StringVal not set")
		}
	}
}

func TestGetAllErrorQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := addQueryTestEntities(c, nil); err != nil {
		t.Fatal(err)
	}

	type errorTestEntity struct {
		SomeValue    int
		AnotherValue int
	}
	vals := []errorTestEntity{}
	q := nds.NewQuery("Entity")
	if _, err := q.GetAll(c, &vals); err == nil {
		t.Fatal("expected error")
	}
}

func TestAncestorQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ancestorKey := nds.NewKey(c, "Ancestor", "parent", 0, nil)
	keys, err := addQueryTestEntities(c, ancestorKey)
	if err != nil {
		t.Fatal(err)
	}

	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
	q = q.Ancestor(ancestorKey)
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count, err := q.Count(c)
	if err != nil {
		t.Fatal(err)
	}

	if count != 5 {
		t.Fatal("count != 5:", count)
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}

	for i, val := range vals {
		if val.Val < 1 || val.Val > 5 {
			t.Fatal("val outside range")
		}
		if keys[i].IntID() != val.Val {
			t.Fatal("key does not match val")
		}
		if !keys[i].Parent().Equal(ancestorKey) {
			t.Fatal("incorrect ancestor key")
		}
	}

	// Test eventual consistency.
	vals = []queryTestEntity{}
	q = nds.NewQuery("Entity")
	q = q.Ancestor(ancestorKey)
	q = q.EventualConsistency()
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count, err = q.Count(c)
	if err != nil {
		t.Fatal(err)
	}

	if count != 5 {
		t.Fatal("count != 5:", count)
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}

	for i, val := range vals {
		if val.Val < 1 || val.Val > 5 {
			t.Fatal("val outside range")
		}
		if keys[i].IntID() != val.Val {
			t.Fatal("key does not match val")
		}
		if !keys[i].Parent().Equal(ancestorKey) {
			t.Fatal("incorrect ancestor key")
		}
	}

}

func TestFilterQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys, err := addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Filter
	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
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

	for i, val := range vals {
		if val.Val < 3 {
			t.Fatal("val outside range")
		}

		if keys[i].IntID() != val.Val {
			t.Fatal("key does not match val")
		}
	}

	q = nds.NewQuery("Entity")
	q = q.Filter("__key__ <", nds.NewKey(c, "Entity", "", 3, nil))

	vals = []queryTestEntity{}

	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 2 {
		t.Fatal("keys != 2")
	}

	if len(vals) != 2 {
		t.Fatal("vals != 2")
	}

	for i, val := range vals {
		if val.Val > 3 {
			t.Fatal("val outside range")
		}
		if keys[i].IntID() != val.Val {
			t.Fatal("key does not match val")
		}
	}
}

func TestOrderQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys, err := addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Ascending order.
	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
	q = q.Order("Val")
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count := int64(1)
	for i, val := range vals {
		if val.Val != count {
			t.Fatal("val.Val is wrong")
		}
		if keys[i].IntID() != count {
			t.Fatal("key is wrong")
		}
		count++
	}

	// Descending order.
	vals = []queryTestEntity{}
	q = nds.NewQuery("Entity")
	q = q.Order("-Val")
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count = int64(5)
	for i, val := range vals {
		if val.Val != count {
			t.Fatal("val.Val is not", count)
		}
		if keys[i].IntID() != count {
			t.Fatal("key is wrong")
		}
		count--
	}

	// Offset ascending order.
	q = nds.NewQuery("Entity")
	q = q.Order("Val")
	q = q.Offset(2)
	vals = []queryTestEntity{}
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	count = int64(3)
	for i, val := range vals {
		if val.Val != count {
			t.Fatal("val.Val is not", count, val.Val)
		}
		if keys[i].IntID() != count {
			t.Fatal("key is wrong")
		}
		count++
	}
}

func TestProjectQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys, err := addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
	q = q.Project("StringVal")
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}

	for i, val := range vals {
		if val.Val != 0 {
			t.Fatal("val not blank")
		}

		if val.StringVal != strconv.FormatInt(keys[i].IntID(), 10) {
			t.Fatal("incorrect StringVal")
		}
	}

	// Test distinct.
	key := nds.NewKey(c, "Entity", "", 6, nil)
	if _, err := nds.Put(c, key, &queryTestEntity{5, "5"}); err != nil {
		t.Fatal(err)
	}
	if err := nds.Get(c, key, &queryTestEntity{}); err != nil {
		t.Fatal(err)
	}

	q = q.Distinct()
	vals = []queryTestEntity{}
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 5 {
		t.Fatal("vals != 5")
	}
}

func TestKeysOnlyQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys, err := addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
	q = q.Order("Val")
	q = q.KeysOnly()
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 5 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 0 {
		t.Fatal("vals != 0")
	}

	for i, key := range keys {
		if key.IntID() != int64(i+1) {
			t.Fatal("incorrect key.IntID()")
		}
	}
}

func TestLimitQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys, err := addQueryTestEntities(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	vals := []queryTestEntity{}
	q := nds.NewQuery("Entity")
	q = q.Order("Val")
	q = q.Limit(2)
	keys, err = q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 2 {
		t.Fatal("keys != 5")
	}

	if len(vals) != 2 {
		t.Fatal("vals != 2")
	}

	for i, key := range keys {
		if key.IntID() != int64(i+1) {
			t.Fatal("incorrect key.IntID()")
		}

		if vals[i].Val != int64(i+1) {
			t.Fatal("incorrect val.Val")
		}
	}
}

func TestIteratorQuery(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := addQueryTestEntities(c, nil); err != nil {
		t.Fatal(err)
	}

	q := nds.NewQuery("Entity")
	q = q.Order("Val")

	startCursorString, endCursorString := "", ""
	iter := q.Run(c)
	for i := int64(1); i < 6; i++ {
		val := queryTestEntity{}
		key, err := iter.Next(&val)
		if err != nil {
			t.Fatal(err)
		}
		if key.IntID() != i {
			t.Fatal("incorrect key")
		}
		if val.Val != i {
			t.Fatal("incorrect val")
		}
		if val.StringVal != strconv.FormatInt(i, 10) {
			t.Fatal("incorrect StringVal")
		}

		if i == 1 {
			cursor, err := iter.Cursor()
			if err != nil {
				t.Fatal(err)
			}
			startCursorString = cursor.String()
		}

		if i == 4 {
			cursor, err := iter.Cursor()
			if err != nil {
				t.Fatal(err)
			}
			endCursorString = cursor.String()
		}
	}
	if _, err := iter.Next(&queryTestEntity{}); err != nds.Done {
		t.Fatal("iter.Next != nds.Done")
	}

	startCursor, err := nds.DecodeCursor(startCursorString)
	if err != nil {
		t.Fatal(err)
	}

	endCursor, err := nds.DecodeCursor(endCursorString)
	if err != nil {
		t.Fatal(err)
	}

	vals := []queryTestEntity{}
	q = nds.NewQuery("Entity")
	q = q.Order("Val")
	q = q.Start(startCursor)
	q = q.End(endCursor)
	keys, err := q.GetAll(c, &vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 3 {
		t.Fatal("keys != 2")
	}

	if len(vals) != 3 {
		t.Fatal("vals != 2")
	}

	for i, key := range keys {
		if key.IntID() != int64(i+2) {
			t.Fatal("incorrect key.IntID()", key.IntID())
		}

		if vals[i].Val != int64(i+2) {
			t.Fatal("incorrect val.Val")
		}
	}

}
