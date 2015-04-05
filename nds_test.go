package nds_test

import (
	"encoding/hex"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/qedus/nds"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"

	"appengine/aetest"
)

type CloseFunc func()

func NewContext(t *testing.T, opts *aetest.Options) (
	context.Context, CloseFunc) {
	c, err := aetest.NewContext(opts)
	if err != nil {
		t.Fatal(err)
	}

	return appengine.NewContext(c.Request().(*http.Request)),
		func() { c.Close() }
}

func TestPutGetDelete(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	// Check we set memcahce, put datastore and delete memcache.
	seq := make(chan string, 3)
	nds.SetMemcacheSetMulti(func(c context.Context,
		items []*memcache.Item) error {
		seq <- "memcache.SetMulti"
		return memcache.SetMulti(c, items)
	})
	nds.SetDatastorePutMulti(func(c context.Context,
		keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
		seq <- "datastore.PutMulti"
		return datastore.PutMulti(c, keys, vals)
	})
	nds.SetMemcacheDeleteMulti(func(c context.Context,
		keys []string) error {
		seq <- "memcache.DeleteMulti"
		close(seq)
		return memcache.DeleteMulti(c, keys)
	})

	incompleteKey := datastore.NewIncompleteKey(c, "Entity", nil)
	key, err := nds.Put(c, incompleteKey, &testEntity{43})
	if err != nil {
		t.Fatal(err)
	}

	nds.SetMemcacheSetMulti(memcache.SetMulti)
	nds.SetDatastorePutMulti(datastore.PutMulti)
	nds.SetMemcacheDeleteMulti(memcache.DeleteMulti)

	if s := <-seq; s != "memcache.SetMulti" {
		t.Fatal("memcache.SetMulti not", s)
	}
	if s := <-seq; s != "datastore.PutMulti" {
		t.Fatal("datastore.PutMulti not", s)
	}
	if s := <-seq; s != "memcache.DeleteMulti" {
		t.Fatal("memcache.DeleteMulti not", s)
	}
	// Check chan is closed.
	<-seq

	if key.Incomplete() {
		t.Fatal("Key is incomplete")
	}

	te := &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.IntVal != 43 {
		t.Fatal("te.Val != 43", te.IntVal)
	}

	// Get from cache.
	te = &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.IntVal != 43 {
		t.Fatal("te.Val != 43", te.IntVal)
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

	if te.IntVal != 64 {
		t.Fatal("te.Val != 64", te.IntVal)
	}

	if err := nds.Delete(c, key); err != nil {
		t.Fatal(err)
	}

	if err := nds.Get(c, key, &testEntity{}); err != datastore.ErrNoSuchEntity {
		t.Fatal("expected datastore.ErrNoSuchEntity")
	}
}

func TestInterfaces(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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

	// Get from nds with struct.
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
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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

	// Get from nds.
	respEntities := make([]testEntity, len(keys))
	err := nds.GetMulti(c, keys, respEntities)
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
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

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

	err := nds.RunInTransaction(c, func(tc context.Context) error {
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
	pl, err := datastore.SaveStruct(&te)
	if err != nil {
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
	if err := datastore.LoadStruct(&loadTestEntity, pl); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(te, loadTestEntity) {
		t.Fatal("entities not equal")
	}
}

func TestMarshalUnmarshalPropertyList(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

	timeVal := time.Now()
	timeProp := datastore.Property{Name: "Time",
		Value: timeVal, NoIndex: false, Multiple: false}

	byteStringVal := datastore.ByteString{0x23}
	byteStringProp := datastore.Property{Name: "ByteString",
		Value: byteStringVal, NoIndex: false, Multiple: false}

	keyVal := datastore.NewKey(c, "Entity", "stringID", 0, nil)
	keyProp := datastore.Property{Name: "Key",
		Value: keyVal, NoIndex: false, Multiple: false}

	blobKeyVal := appengine.BlobKey("blobkey")
	blobKeyProp := datastore.Property{Name: "BlobKey",
		Value: blobKeyVal, NoIndex: false, Multiple: false}

	geoPointVal := appengine.GeoPoint{1, 2}
	geoPointProp := datastore.Property{Name: "GeoPoint",
		Value: geoPointVal, NoIndex: false, Multiple: false}

	pl := datastore.PropertyList{
		timeProp,
		byteStringProp,
		keyProp,
		blobKeyProp,
		geoPointProp,
	}
	data, err := nds.MarshalPropertyList(pl)
	if err != nil {
		t.Fatal(err)
	}

	testEntity := &struct {
		Time       time.Time
		ByteString datastore.ByteString
		Key        *datastore.Key
		BlobKey    appengine.BlobKey
		GeoPoint   appengine.GeoPoint
	}{}

	pl = datastore.PropertyList{}
	if err := nds.UnmarshalPropertyList(data, &pl); err != nil {
		t.Fatal(err)
	}
	if err := nds.SetValue(reflect.ValueOf(testEntity), pl); err != nil {
		t.Fatal(err)
	}

	if !testEntity.Time.Equal(timeVal) {
		t.Fatal("timeVal not equal")
	}

	if string(testEntity.ByteString) != string(byteStringVal) {
		t.Fatal("byteStringVal not equal")
	}

	if !testEntity.Key.Equal(keyVal) {
		t.Fatal("keyVal not equal")
	}

	if testEntity.BlobKey != blobKeyVal {
		t.Fatal("blobKeyVal not equal")
	}

	if !reflect.DeepEqual(testEntity.GeoPoint, geoPointVal) {
		t.Fatal("geoPointVal not equal")
	}
}

func TestMartialPropertyListError(t *testing.T) {

	type testEntity struct {
		IntVal int
	}

	pl := datastore.PropertyList{
		datastore.Property{"Prop", &testEntity{3}, false, false},
	}
	if _, err := nds.MarshalPropertyList(pl); err == nil {
		t.Fatal("expected error")
	}
}

func randHexString(length int) string {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(rand.Int())
	}
	return hex.EncodeToString(bytes)
}

func TestCreateMemcacheKey(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
	defer closeFunc()

	// Check keys are hashed over nds.MemcacheMaxKeySize.
	maxKeySize := nds.MemcacheMaxKeySize
	key := datastore.NewKey(c, "TestEntity",
		randHexString(maxKeySize+10), 0, nil)

	memcacheKey := nds.CreateMemcacheKey(key)
	if len(memcacheKey) > maxKeySize {
		t.Fatal("incorrect memcache key size")
	}
}
