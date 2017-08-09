package nds_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/qedus/nds"

	"errors"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

func TestGetMultiStruct(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}

	// Get from cache.
	response = make([]testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}
}

func TestGetMultiStructPtr(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	if err := nds.GetMulti(c, keys, response); err != nil {
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
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}
}

func TestGetMultiStructPtrNil(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}

	// Get from cache.
	response = make([]*testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if response[i].IntVal != i+1 {
			t.Fatal("incorrect IntVal")
		}
	}
}

func TestGetMultiInterface(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	if err := nds.GetMulti(c, keys, response); err != nil {
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
	if err := nds.GetMulti(c, keys, response); err != nil {
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

func TestGetMultiPropertyLoadSaver(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	keys := []*datastore.Key{}
	entities := []datastore.PropertyList{}

	for i := 1; i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", int64(i), nil))

		pl, err := datastore.SaveStruct(&testEntity{i})
		if err != nil {
			t.Fatal(err)
		}
		entities = append(entities, pl)
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Prime the cache.
	uncachedEntities := make([]datastore.PropertyList, len(keys))
	if err := nds.GetMulti(c, keys, uncachedEntities); err != nil {
		t.Fatal(err)
	}

	for i, e := range entities {
		if !reflect.DeepEqual(e, uncachedEntities[i]) {
			t.Fatal("uncachedEntities not equal", e, uncachedEntities[i])
		}
	}

	// Use cache.
	cachedEntities := make([]datastore.PropertyList, len(keys))
	if err := nds.GetMulti(c, keys, cachedEntities); err != nil {
		t.Fatal(err)
	}

	for i, e := range entities {
		if !reflect.DeepEqual(e, cachedEntities[i]) {
			t.Fatal("cachedEntities not equal", e, cachedEntities[i])
		}
	}

	// We know the datastore supports property load saver but we need to make
	// sure that memcache does by ensuring memcache does not error when we
	// change to fetching with structs.
	// Do this by making sure the datastore is not called on this following
	// GetMulti as memcache should have worked.
	nds.SetDatastoreGetMulti(func(c context.Context,
		keys []*datastore.Key, vals interface{}) error {
		if len(keys) != 0 {
			return errors.New("should not be called")
		}
		return nil
	})
	defer func() {
		nds.SetDatastoreGetMulti(datastore.GetMulti)
	}()
	tes := make([]testEntity, len(entities))
	if err := nds.GetMulti(c, keys, tes); err != nil {
		t.Fatal(err)
	}
}

func TestGetMultiNoKeys(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}

	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
}

func TestGetMultiInterfaceError(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	if err := nds.GetMulti(c, keys, response); err != nil {
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
	if err := nds.GetMulti(c, keys, response); err == nil {
		t.Fatal("expected invalid entity type error")
	}
}

// This is just used to ensure interfaces don't currently work.
type readerTestEntity struct {
	IntVal int
}

func (rte readerTestEntity) Read(p []byte) (n int, err error) {
	return 1, nil
}

var _ io.Reader = readerTestEntity{}

func newReaderTestEntity() io.Reader {
	return readerTestEntity{}
}

func TestGetArgs(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int64
	}

	if err := nds.Get(c, nil, &testEntity{}); err == nil {
		t.Fatal("expected error for nil key")
	}

	key := datastore.NewKey(c, "Entity", "", 1, nil)
	if err := nds.Get(c, key, nil); err != datastore.ErrInvalidEntityType {
		t.Fatal("expected ErrInvalidEntityType for nil value")
	}

	if err := nds.Get(c, key, datastore.PropertyList{}); err == nil {
		t.Fatal("expected error for datastore.PropertyList")
	}

	if err := nds.Get(c, key, testEntity{}); err == nil {
		t.Fatal("expected error for struct")
	}

	rte := newReaderTestEntity()
	if err := nds.Get(c, key, rte); err == nil {
		t.Fatal("expected error for interface")
	}
}

func TestGetMultiArgs(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int64
	}

	key := datastore.NewKey(c, "Entity", "", 1, nil)
	keys := []*datastore.Key{key}
	val := testEntity{}
	if err := nds.GetMulti(c, keys, nil); err == nil {
		t.Fatal("expected error for nil vals")
	}
	structVals := []testEntity{val}
	if err := nds.GetMulti(c, nil, structVals); err == nil {
		t.Fatal("expected error for nil keys")
	}

	if err := nds.GetMulti(c, keys, []testEntity{}); err == nil {
		t.Fatal("expected error for unequal keys and vals")
	}

	if err := nds.GetMulti(c, keys, datastore.PropertyList{}); err == nil {
		t.Fatal("expected error for propertyList")
	}

	rte := newReaderTestEntity()
	if err := nds.GetMulti(c, keys, []io.Reader{rte}); err == nil {
		t.Fatal("expected error for interface")
	}
}

func TestGetSliceProperty(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVals []int64
	}

	key := datastore.NewKey(c, "Entity", "", 1, nil)
	intVals := []int64{0, 1, 2, 3}
	val := &testEntity{intVals}

	if _, err := nds.Put(c, key, val); err != nil {
		t.Fatal(err)
	}

	// Get from datastore.
	newVal := &testEntity{}
	if err := nds.Get(c, key, newVal); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(val.IntVals, intVals) {
		t.Fatal("slice properties not equal", val.IntVals)
	}

	// Get from memcache.
	newVal = &testEntity{}
	if err := nds.Get(c, key, newVal); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(val.IntVals, intVals) {
		t.Fatal("slice properties not equal", val.IntVals)
	}
}

func TestGetMultiNoPropertyList(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	pl := datastore.PropertyList{datastore.Property{}}

	if err := nds.GetMulti(c, keys, pl); err == nil {
		t.Fatal("expecting no PropertyList error")
	}
}

func TestGetMultiNonStruct(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	keys := []*datastore.Key{datastore.NewKey(c, "Test", "", 1, nil)}
	vals := []int{12}

	if err := nds.GetMulti(c, keys, vals); err == nil {
		t.Fatal("expecting unsupported vals type")
	}
}

func TestGetMultiLockReturnEntitySetValueFail(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	// Fail to unmarshal test.
	memcacheGetChan := make(chan func(c context.Context, keys []string) (
		map[string]*memcache.Item, error), 2)
	memcacheGetChan <- memcache.GetMulti
	memcacheGetChan <- func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		items, err := memcache.GetMulti(c, keys)
		if err != nil {
			return nil, err
		}
		pl := datastore.PropertyList{
			datastore.Property{"One", 1, false, false},
		}
		value, err := nds.MarshalPropertyList(pl)
		if err != nil {
			return nil, err
		}
		items[keys[0]].Flags = nds.EntityItem
		items[keys[0]].Value = value
		items[keys[1]].Flags = nds.EntityItem
		items[keys[1]].Value = value
		return items, nil
	}
	nds.SetMemcacheGetMulti(func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		f := <-memcacheGetChan
		return f(c, keys)
	})

	response := make([]testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	defer nds.SetMemcacheGetMulti(memcache.GetMulti)

	for i := 0; i < len(keys); i++ {
		if entities[i].IntVal != response[i].IntVal {
			t.Fatal("IntVal not equal")
		}
	}
}

func TestGetMultiLockReturnEntity(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	memcacheGetChan := make(chan func(c context.Context, keys []string) (
		map[string]*memcache.Item, error), 2)
	memcacheGetChan <- memcache.GetMulti
	memcacheGetChan <- func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		items, err := memcache.GetMulti(c, keys)
		if err != nil {
			return nil, err
		}
		pl := datastore.PropertyList{
			datastore.Property{"IntVal", int64(5), false, false},
		}
		value, err := nds.MarshalPropertyList(pl)
		if err != nil {
			return nil, err
		}
		items[keys[0]].Flags = nds.EntityItem
		items[keys[0]].Value = value
		items[keys[1]].Flags = nds.EntityItem
		items[keys[1]].Value = value
		return items, nil
	}
	nds.SetMemcacheGetMulti(func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		f := <-memcacheGetChan
		return f(c, keys)
	})

	response := make([]testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	defer nds.SetMemcacheGetMulti(memcache.GetMulti)

	for i := 0; i < len(keys); i++ {
		if 5 != response[i].IntVal {
			t.Fatal("IntVal not equal")
		}
	}
}

func TestGetMultiLockReturnUnknown(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	memcacheGetChan := make(chan func(c context.Context, keys []string) (
		map[string]*memcache.Item, error), 2)
	memcacheGetChan <- memcache.GetMulti
	memcacheGetChan <- func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		items, err := memcache.GetMulti(c, keys)
		if err != nil {
			return nil, err
		}

		// Unknown lock values.
		items[keys[0]].Flags = 23
		items[keys[1]].Flags = 24
		return items, nil
	}
	nds.SetMemcacheGetMulti(func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		f := <-memcacheGetChan
		return f(c, keys)
	})

	response := make([]testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	defer nds.SetMemcacheGetMulti(memcache.GetMulti)

	for i := 0; i < len(keys); i++ {
		if entities[i].IntVal != response[i].IntVal {
			t.Fatal("IntVal not equal")
		}
	}
}

func TestGetMultiLockReturnMiss(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

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

	memcacheGetChan := make(chan func(c context.Context, keys []string) (
		map[string]*memcache.Item, error), 2)
	memcacheGetChan <- memcache.GetMulti
	memcacheGetChan <- func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		items, err := memcache.GetMulti(c, keys)
		if err != nil {
			return nil, err
		}

		// Remove one item between memcache Add and Get.
		delete(items, keys[0])
		return items, nil
	}
	nds.SetMemcacheGetMulti(func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		f := <-memcacheGetChan
		return f(c, keys)
	})

	response := make([]testEntity, len(keys))
	if err := nds.GetMulti(c, keys, response); err != nil {
		t.Fatal(err)
	}
	defer nds.SetMemcacheGetMulti(memcache.GetMulti)

	for i := 0; i < len(keys); i++ {
		if entities[i].IntVal != response[i].IntVal {
			t.Fatal("IntVal not equal")
		}
	}
}

// TestGetNamespacedKey ensures issue https://goo.gl/rXU8nK is fixed so that
// memcache uses the namespace from the key instead of the context.
func TestGetNamespacedKey(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	const intVal = int64(12)
	type testEntity struct {
		IntVal int64
	}

	namespacedCtx, err := appengine.Namespace(c, "keyNamespace")
	if err != nil {
		t.Fatal(err)
	}

	key := datastore.NewKey(c, "Entity", "", 1, nil)
	namespacedKey := datastore.NewKey(namespacedCtx,
		"Entity", "", key.IntID(), nil)
	entity := &testEntity{intVal}

	if namespacedKey, err = nds.Put(c, namespacedKey, entity); err != nil {
		t.Fatal(err)
	}

	// Prime cache.
	if err := nds.Get(namespacedCtx, namespacedKey, &testEntity{}); err != nil {
		t.Fatal(err)
	}

	// Ensure that we get a value back from the cache by checking if the
	// datastore is called at all.
	entityFromCache := true
	nds.SetDatastoreGetMulti(func(c context.Context,
		keys []*datastore.Key, vals interface{}) error {
		if len(keys) != 0 {
			entityFromCache = false
		}
		return nil
	})
	if err := nds.Get(c, namespacedKey, &testEntity{}); err != nil {
		t.Fatal(err)
	}
	nds.SetDatastoreGetMulti(datastore.GetMulti)

	if !entityFromCache {
		t.Fatal("entity not obtained from cache")
	}

	if err := nds.Delete(namespacedCtx, namespacedKey); err != nil {
		t.Fatal(err)
	}

	entity = &testEntity{}
	if err := nds.Get(c, namespacedKey, entity); err == nil {
		t.Fatalf("expected no such entity error but got %+v", entity)
	} else if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func TestGetMultiPaths(t *testing.T) {
	expectedErr := errors.New("expected error")

	type memcacheGetMultiFunc func(c context.Context,
		keys []string) (map[string]*memcache.Item, error)
	memcacheGetMultiFail := func(c context.Context,
		keys []string) (map[string]*memcache.Item, error) {
		return nil, expectedErr
	}

	type memcacheAddMultiFunc func(c context.Context,
		items []*memcache.Item) error
	memcacheAddMultiFail := func(c context.Context,
		items []*memcache.Item) error {
		return expectedErr
	}

	type memcacheCompareAndSwapMultiFunc func(c context.Context,
		items []*memcache.Item) error
	memcacheCompareAndSwapMultiFail := func(c context.Context,
		items []*memcache.Item) error {
		return expectedErr
	}

	type datastoreGetMultiFunc func(c context.Context,
		keys []*datastore.Key, vals interface{}) error
	datastoreGetMultiFail := func(c context.Context,
		keys []*datastore.Key, vals interface{}) error {
		return expectedErr
	}

	type marshalFunc func(pl datastore.PropertyList) ([]byte, error)
	marshalFail := func(pl datastore.PropertyList) ([]byte, error) {
		return nil, expectedErr
	}

	type unmarshalFunc func(data []byte, pl *datastore.PropertyList) error
	/*
	   unmarshalFail := func(data []byte, pl *datastore.PropertyList) error {
	       return expectedErr
	   }
	*/

	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int64
	}

	keysVals := func(c context.Context, count int64) (
		[]*datastore.Key, []testEntity) {

		keys, vals := make([]*datastore.Key, count), make([]testEntity, count)
		for i := int64(0); i < count; i++ {
			keys[i] = datastore.NewKey(c, "Entity", "", i+1, nil)
			vals[i] = testEntity{i + 1}
		}
		return keys, vals
	}

	tests := []struct {
		description string

		// Number of keys used to as GetMulti params.
		keyCount int64

		// Number of times GetMulti is called.
		callCount int

		// There are 2 memcacheGetMulti calls for every GetMulti call.
		memcacheGetMultis           []memcacheGetMultiFunc
		memcacheAddMulti            memcacheAddMultiFunc
		memcacheCompareAndSwapMulti memcacheCompareAndSwapMultiFunc

		datastoreGetMulti datastoreGetMultiFunc

		marshal marshalFunc
		// There are 2 unmarshal calls for every GetMultiCall.
		unmarshals []unmarshalFunc

		expectedErrs []error
	}{
		{
			"no errors",
			20,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
		{
			"datastore unknown error",
			2,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastoreGetMultiFail,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{expectedErr},
		},
		{
			"datastore unknown multierror",
			2,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			func(c context.Context,
				keys []*datastore.Key, vals interface{}) error {

				me := make(appengine.MultiError, len(keys))
				for i := range me {
					me[i] = expectedErr
				}
				return me
			},
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{
				appengine.MultiError{expectedErr, expectedErr},
			},
		},
		{
			"marshal error",
			5,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			marshalFail,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
		{
			"total memcache fail",
			20,
			1,
			[]memcacheGetMultiFunc{
				memcacheGetMultiFail,
				memcacheGetMultiFail,
			},
			memcacheAddMultiFail,
			memcacheCompareAndSwapMultiFail,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
		{
			"lock memcache fail",
			20,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				memcacheGetMultiFail,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
		{
			"memcache corrupt",
			2,
			2,
			[]memcacheGetMultiFunc{
				// Charge memcache.
				memcache.GetMulti,
				memcache.GetMulti,
				// Corrupt memcache.
				func(c context.Context, keys []string) (
					map[string]*memcache.Item, error) {
					items, err := memcache.GetMulti(c, keys)
					// Corrupt items.
					for _, item := range items {
						item.Value = []byte("corrupt string")
					}
					return items, err
				},
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil, nil},
		},
		{
			"memcache flag corrupt",
			2,
			2,
			[]memcacheGetMultiFunc{
				// Charge memcache.
				memcache.GetMulti,
				memcache.GetMulti,
				// Corrupt memcache flags.
				func(c context.Context, keys []string) (
					map[string]*memcache.Item, error) {
					items, err := memcache.GetMulti(c, keys)
					// Corrupt flags with unknown number.
					for _, item := range items {
						item.Flags = 56
					}
					return items, err
				},
				memcache.GetMulti,
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil, nil},
		},
		{
			"lock memcache value fail",
			20,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				func(c context.Context, keys []string) (
					map[string]*memcache.Item, error) {
					items, err := memcache.GetMulti(c, keys)
					// Corrupt flags with unknown number.
					for _, item := range items {
						item.Value = []byte("corrupt value")
					}
					return items, err
				},
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
		{
			"lock memcache value none item",
			2,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				func(c context.Context, keys []string) (
					map[string]*memcache.Item, error) {
					items, err := memcache.GetMulti(c, keys)
					// Corrupt flags with unknown number.
					for _, item := range items {
						item.Flags = nds.NoneItem
					}
					return items, err
				},
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{
				appengine.MultiError{
					datastore.ErrNoSuchEntity,
					datastore.ErrNoSuchEntity,
				},
			},
		},
		{
			"memcache get no entity unmarshal fail",
			2,
			1,
			[]memcacheGetMultiFunc{
				memcache.GetMulti,
				func(c context.Context, keys []string) (
					map[string]*memcache.Item, error) {
					items, err := memcache.GetMulti(c, keys)
					// Corrupt flags with unknown number.
					for _, item := range items {
						item.Flags = nds.EntityItem
					}
					return items, err
				},
			},
			memcache.AddMulti,
			memcache.CompareAndSwapMulti,
			datastore.GetMulti,
			nds.MarshalPropertyList,
			[]unmarshalFunc{
				nds.UnmarshalPropertyList,
				nds.UnmarshalPropertyList,
			},
			[]error{nil},
		},
	}

	for _, test := range tests {
		t.Log("Start", test.description)

		keys, putVals := keysVals(c, test.keyCount)
		if _, err := nds.PutMulti(c, keys, putVals); err != nil {
			t.Fatal(err)
		}

		memcacheGetChan := make(chan memcacheGetMultiFunc,
			len(test.memcacheGetMultis))

		for _, fn := range test.memcacheGetMultis {
			memcacheGetChan <- fn
		}

		nds.SetMemcacheGetMulti(func(c context.Context, keys []string) (
			map[string]*memcache.Item, error) {
			fn := <-memcacheGetChan
			return fn(c, keys)
		})

		nds.SetMemcacheAddMulti(test.memcacheAddMulti)
		nds.SetMemcacheCompareAndSwapMulti(test.memcacheCompareAndSwapMulti)

		nds.SetDatastoreGetMulti(test.datastoreGetMulti)

		nds.SetMarshal(test.marshal)

		unmarshalChan := make(chan unmarshalFunc,
			len(test.unmarshals))

		for _, fn := range test.unmarshals {
			unmarshalChan <- fn
		}

		nds.SetUnmarshal(func(data []byte, pl *datastore.PropertyList) error {
			fn := <-unmarshalChan
			return fn(data, pl)
		})

		for i := 0; i < test.callCount; i++ {
			getVals := make([]testEntity, test.keyCount)
			err := nds.GetMulti(c, keys, getVals)

			expectedErr := test.expectedErrs[i]

			if expectedErr == nil {
				if err != nil {
					t.Fatal(err)
				}

				for i := range getVals {
					if getVals[i].IntVal != putVals[i].IntVal {
						t.Fatal("incorrect IntVal")
					}
				}
				continue
			}

			if err == nil {
				t.Fatal("expected error")
			}
			expectedMultiErr, isMultiErr := expectedErr.(appengine.MultiError)

			if isMultiErr {
				me, ok := err.(appengine.MultiError)
				if !ok {
					t.Fatal("expected appengine.MultiError but got", err)
				}

				if len(me) != len(expectedMultiErr) {
					t.Fatal("appengine.MultiError length incorrect")
				}

				for i, e := range me {
					if e != expectedMultiErr[i] {
						t.Fatal("non matching errors", e, expectedMultiErr[i])
					}

					if e == nil {
						if getVals[i].IntVal != putVals[i].IntVal {
							t.Fatal("incorrect IntVal")
						}
					}
				}
			}
		}

		// Reset App Engine API calls.
		nds.SetMemcacheGetMulti(memcache.GetMulti)
		nds.SetMemcacheAddMulti(memcache.AddMulti)
		nds.SetMemcacheCompareAndSwapMulti(memcache.CompareAndSwapMulti)
		nds.SetDatastoreGetMulti(datastore.GetMulti)
		nds.SetMarshal(nds.MarshalPropertyList)
		nds.SetUnmarshal(nds.UnmarshalPropertyList)

		if err := nds.DeleteMulti(c, keys); err != nil {
			t.Fatal(err)
		}
		t.Log("End", test.description)
	}
}

type loadSaveStruct struct {
	Value int64
}

func (lss *loadSaveStruct) Save() ([]datastore.Property, error) {
	return []datastore.Property{
		datastore.Property{
			Name:     "Val",
			Value:    lss.Value,
			NoIndex:  true,
			Multiple: false,
		},
	}, nil
}

func (lss *loadSaveStruct) Load(properties []datastore.Property) error {

	for _, p := range properties {
		if p.Name == "Val" {
			lss.Value = p.Value.(int64)
		}
	}
	return nil
}

func TestPropertyLoadSaver(t *testing.T) {
	ctx, closeFunc := NewContext(t)
	defer closeFunc()

	keys := []*datastore.Key{
		datastore.NewIncompleteKey(ctx, "Entity", nil),
	}
	entities := []*loadSaveStruct{
		&loadSaveStruct{
			Value: 23,
		},
	}
	keys, err := nds.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatal(err)
	}

	entities = make([]*loadSaveStruct, 1)
	if err := nds.GetMulti(ctx, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].Value != 23 {
		t.Fatal("expected another value")
	}
}

func TestUnsupportedValueType(t *testing.T) {
	ctx, closeFunc := NewContext(t)
	defer closeFunc()

	keys := []*datastore.Key{
		datastore.NewIncompleteKey(ctx, "Entity", nil),
	}
	entities := make([]int, 1)
	if err := nds.GetMulti(ctx, keys, entities); err == nil {
		t.Fatal("expected unsupported value error")
	}
}

func TestGetMultiFieldMismatch(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal  int64
		IntVal2 int64
	}

	type testEntityLean struct {
		IntVal int64
	}

	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := int64(1); i < 3; i++ {
		keys = append(keys, datastore.NewKey(c, "Entity", "", i, nil))
		entities = append(entities, testEntity{i, i})
	}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from datastore using nds.
	ndsResponse := make([]testEntityLean, len(keys))
	ndsErr := nds.GetMulti(c, keys, ndsResponse)

	// Get from datastore using google api
	dsResponse := make([]testEntityLean, len(keys))
	dsErr := datastore.GetMulti(c, keys, dsResponse)

	if ndsErr.Error() != dsErr.Error() {
		t.Fatal("Errors are not equal")
	}

	for i := int64(0); i < 2; i++ {
		if ndsResponse[i].IntVal != dsResponse[i].IntVal {
			t.Fatalf("IntVals are not equal %d %d", ndsResponse[i].IntVal, dsResponse[i].IntVal)
		}
	}
}
