package nds_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds"
)

func TestGetSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestGetMultiStruct", GetMultiStructTest(item.ctx, item.cacher))
			t.Run("TestGetMultiStructPtr", GetMultiStructPtrTest(item.ctx, item.cacher))
			t.Run("TestGetMultiStructPtrNil", GetMultiStructPtrNilTest(item.ctx, item.cacher))
			t.Run("TestGetMultiInterface", GetMultiInterfaceTest(item.ctx, item.cacher))
			t.Run("TestGetMultiPropertyLoadSaver", GetMultiPropertyLoadSaverTest(item.ctx, item.cacher))
			t.Run("TestGetMultiNoKeys", GetMultiNoKeysTest(item.ctx, item.cacher))
			t.Run("TestGetMultiInterfaceError", GetMultiInterfaceErrorTest(item.ctx, item.cacher))
			t.Run("TestGetArgs", GetArgsTest(item.ctx, item.cacher))
			t.Run("TestGetMultiArgs", GetMultiArgsTest(item.ctx, item.cacher))
			t.Run("TestGetSliceProperty", GetSlicePropertyTest(item.ctx, item.cacher))
			t.Run("TestGetMultiNoPropertyList", GetMultiNoPropertyListTest(item.ctx, item.cacher))
			t.Run("TestGetMultiNonStruct", GetMultiNonStructTest(item.ctx, item.cacher))
			t.Run("TestGetMultiLockReturnEntitySetValueFail", GetMultiLockReturnEntitySetValueFailTest(item.ctx, item.cacher))
			t.Run("TestGetMultiLockReturnEntity", GetMultiLockReturnEntityTest(item.ctx, item.cacher))
			t.Run("TestGetMultiLockReturnUnknown", GetMultiLockReturnUnknownTest(item.ctx, item.cacher))
			t.Run("TestGetMultiLockReturnMiss", GetMultiLockReturnMissTest(item.ctx, item.cacher))
			t.Run("TestGetNamespacedKey", GetNamespacedKeyTest(item.ctx, item.cacher))
			t.Run("TestGetMultiPaths", GetMultiPathsTest(item.ctx, item.cacher))
			t.Run("TestPropertyLoadSaver", PropertyLoadSaverTest(item.ctx, item.cacher))
			t.Run("TestUnsupportedValueType", UnsupportedValueTypeTest(item.ctx, item.cacher))
			t.Run("TestGetMultiFieldMismatch", GetMultiFieldMismatchTest(item.ctx, item.cacher))
		})
	}
}

func GetMultiStructTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiStructTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}

		// Get from cache.
		response = make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiStructPtrTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiStructPtrTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]*testEntity, len(keys))
		for i := 0; i < len(response); i++ {
			response[i] = &testEntity{}
		}

		if err := ndsClient.GetMulti(c, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiStructPtrNilTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiStructPtrNilTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]*testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}

		// Get from cache.
		response = make([]*testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiInterfaceTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiInterfaceTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]interface{}, len(keys))
		for i := 0; i < len(response); i++ {
			response[i] = &testEntity{}
		}

		if err := ndsClient.GetMulti(c, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
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
}

func GetMultiPropertyLoadSaverTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		keys := []*datastore.Key{}
		entities := []datastore.PropertyList{}

		for i := 1; i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiPropertyLoadSaverTest", int64(i), nil))

			pl, err := datastore.SaveStruct(&testEntity{i})
			if err != nil {
				t.Fatal(err)
			}
			entities = append(entities, pl)
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Prime the cache.
		uncachedEntities := make([]datastore.PropertyList, len(keys))
		if err := ndsClient.GetMulti(c, keys, uncachedEntities); err != nil {
			t.Fatal(err)
		}

		for i, e := range entities {
			if !reflect.DeepEqual(e, uncachedEntities[i]) {
				t.Fatal("uncachedEntities not equal", e, uncachedEntities[i])
			}
		}

		// Use cache.
		cachedEntities := make([]datastore.PropertyList, len(keys))
		if err := ndsClient.GetMulti(c, keys, cachedEntities); err != nil {
			t.Fatal(err)
		}

		for i, e := range entities {
			if !reflect.DeepEqual(e, cachedEntities[i]) {
				t.Fatal("cachedEntities not equal", e, cachedEntities[i])
			}
		}

		// We know the datastore supports property load saver but we need to make
		// sure that the cache does by ensuring the cacher does not error when we
		// change to fetching with structs.
		// Do this by making sure the datastore is not called on this following
		// GetMulti as the cache should have worked.
		nds.SetDatastoreGetMultiHook(func(c context.Context,
			keys []*datastore.Key, vals interface{}) error {
			if len(keys) != 0 {
				return errors.New("should not be called")
			}
			return nil
		})
		defer nds.SetDatastoreGetMultiHook(nil)

		tes := make([]testEntity, len(entities))
		if err := ndsClient.GetMulti(c, keys, tes); err != nil {
			t.Fatal(err)
		}
	}
}

func GetMultiNoKeysTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}

		if err := ndsClient.GetMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}
	}
}

func GetMultiInterfaceErrorTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiInterfaceErrorTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		// No errors expected.
		response := []interface{}{&testEntity{}, &testEntity{}}

		if err := ndsClient.GetMulti(c, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(c, keys, response); err == nil {
			t.Fatal("expected invalid entity type error")
		}
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

func GetArgsTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		if err := ndsClient.Get(c, nil, &testEntity{}); err == nil {
			t.Fatal("expected error for nil key")
		}

		key := datastore.IDKey("GetArgsTest", 1, nil)
		if err := ndsClient.Get(c, key, nil); err != datastore.ErrInvalidEntityType {
			t.Fatal("expected ErrInvalidEntityType for nil value")
		}

		if err := ndsClient.Get(c, key, datastore.PropertyList{}); err == nil {
			t.Fatal("expected error for datastore.PropertyList")
		}

		if err := ndsClient.Get(c, key, testEntity{}); err == nil {
			t.Fatal("expected error for struct")
		}

		rte := newReaderTestEntity()
		if err := ndsClient.Get(c, key, rte); err == nil {
			t.Fatal("expected error for interface")
		}
	}
}

func GetMultiArgsTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		key := datastore.IDKey("GetMultiArgsTest", 1, nil)
		keys := []*datastore.Key{key}
		val := testEntity{}
		if err := ndsClient.GetMulti(c, keys, nil); err == nil {
			t.Fatal("expected error for nil vals")
		}
		structVals := []testEntity{val}
		if err := ndsClient.GetMulti(c, nil, structVals); err == nil {
			t.Fatal("expected error for nil keys")
		}

		if err := ndsClient.GetMulti(c, keys, []testEntity{}); err == nil {
			t.Fatal("expected error for unequal keys and vals")
		}

		if err := ndsClient.GetMulti(c, keys, datastore.PropertyList{}); err == nil {
			t.Fatal("expected error for propertyList")
		}

		rte := newReaderTestEntity()
		if err := ndsClient.GetMulti(c, keys, []io.Reader{rte}); err == nil {
			t.Fatal("expected error for interface")
		}
	}
}

func GetSlicePropertyTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVals []int64
		}

		key := datastore.IDKey("GetSlicePropertyTest", 1, nil)
		intVals := []int64{0, 1, 2, 3}
		val := &testEntity{intVals}

		if _, err := ndsClient.Put(c, key, val); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		newVal := &testEntity{}
		if err := ndsClient.Get(c, key, newVal); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(val.IntVals, intVals) {
			t.Fatal("slice properties not equal", val.IntVals)
		}

		// Get from cache.
		newVal = &testEntity{}
		if err := ndsClient.Get(c, key, newVal); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(val.IntVals, intVals) {
			t.Fatal("slice properties not equal", val.IntVals)
		}
	}
}

func GetMultiNoPropertyListTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("GetMultiNoPropertyListTest", 1, nil)}
		pl := datastore.PropertyList{datastore.Property{}}

		if err := ndsClient.GetMulti(c, keys, pl); err == nil {
			t.Fatal("expecting no PropertyList error")
		}
	}
}

func GetMultiNonStructTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("GetMultiNonStructTest", 1, nil)}
		vals := []int{12}

		if err := ndsClient.GetMulti(c, keys, vals); err == nil {
			t.Fatal("expecting unsupported vals type")
		}
	}
}

func GetMultiLockReturnEntitySetValueFailTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(c, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiLockReturnEntitySetValueFailTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Fail to unmarshal test.
		cacheGetChan := make(chan func(c context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(c, keys)
			if err != nil {
				return nil, err
			}
			pl := datastore.PropertyList{
				datastore.Property{Name: "One", Value: 1, NoIndex: false},
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
		close(cacheGetChan)

		testCacher.getMultiHook = func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(c, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnEntityTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(c, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiLockReturnEntityTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(c context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(c, keys)
			if err != nil {
				return nil, err
			}
			pl := datastore.PropertyList{
				datastore.Property{Name: "IntVal", Value: int64(5), NoIndex: false},
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
		testCacher.getMultiHook = func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(c, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if 5 != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnUnknownTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(c, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiLockReturnUnknownTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(c context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(c, keys)
			if err != nil {
				return nil, err
			}

			// Unknown lock values.
			items[keys[0]].Flags = 23
			items[keys[1]].Flags = 24
			return items, nil
		}
		testCacher.getMultiHook = func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(c, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnMissTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(c, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiLockReturnMissTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(c context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(c, keys)
			if err != nil {
				return nil, err
			}

			// Remove one item between the cache Add and Get.
			delete(items, keys[0])
			return items, nil
		}
		testCacher.getMultiHook = func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(c, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(c, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

// TestGetNamespacedKey ensures issue https://goo.gl/rXU8nK is fixed so that
// the cache uses the namespace from the key instead of the context.
func GetNamespacedKeyTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		const intVal = int64(12)
		const ns = "keyNamespace"
		type testEntity struct {
			IntVal int64
		}

		key := datastore.IDKey("GetNamespacedKeyTest", 1, nil)
		namespacedKey := datastore.IDKey("GetNamespacedKeyTest", key.ID, nil)
		namespacedKey.Namespace = ns
		entity := &testEntity{intVal}

		if namespacedKey, err = ndsClient.Put(c, namespacedKey, entity); err != nil {
			t.Fatal(err)
		}

		// Prime cache.
		if err := ndsClient.Get(c, namespacedKey, &testEntity{}); err != nil {
			t.Fatal(err)
		}

		// Ensure that we get a value back from the cache by checking if the
		// datastore is called at all.
		entityFromCache := true
		nds.SetDatastoreGetMultiHook(func(c context.Context,
			keys []*datastore.Key, vals interface{}) error {
			if len(keys) != 0 {
				entityFromCache = false
			}
			return nil
		})
		if err := ndsClient.Get(c, namespacedKey, &testEntity{}); err != nil {
			t.Fatal(err)
		}
		nds.SetDatastoreGetMultiHook(nil)

		if !entityFromCache {
			t.Fatal("entity not obtained from cache")
		}

		if err := ndsClient.Delete(c, namespacedKey); err != nil {
			t.Fatal(err)
		}

		entity = &testEntity{}
		if err := ndsClient.Get(c, namespacedKey, entity); err == nil {
			t.Fatalf("expected no such entity error but got %+v", entity)
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}
	}
}

func GetMultiPathsTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}

		expectedErr := errors.New("expected error")

		type cacheGetMultiFunc func(c context.Context,
			keys []string) (map[string]*nds.Item, error)
		cacheGetMultiFail := func(c context.Context,
			keys []string) (map[string]*nds.Item, error) {
			return nil, expectedErr
		}

		type cacheAddMultiFunc func(c context.Context,
			items []*nds.Item) error
		cacheAddMultiFail := func(c context.Context,
			items []*nds.Item) error {
			return expectedErr
		}

		type cacheCompareAndSwapMultiFunc func(c context.Context,
			items []*nds.Item) error
		cacheCompareAndSwapMultiFail := func(c context.Context,
			items []*nds.Item) error {
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

		type testEntity struct {
			IntVal int64
		}

		keysVals := func(c context.Context, count int64) (
			[]*datastore.Key, []testEntity) {

			keys, vals := make([]*datastore.Key, count), make([]testEntity, count)
			for i := int64(0); i < count; i++ {
				keys[i] = datastore.IDKey("GetMultiPathsTest", i+1, nil)
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

			// There are 2 cacheGetMulti calls for every GetMulti call.
			cacheGetMultis           []cacheGetMultiFunc
			cacheAddMulti            cacheAddMultiFunc
			cacheCompareAndSwapMulti cacheCompareAndSwapMultiFunc

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
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
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
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
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
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				func(c context.Context,
					keys []*datastore.Key, vals interface{}) error {

					me := make(datastore.MultiError, len(keys))
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
					datastore.MultiError{expectedErr, expectedErr},
				},
			},
			{
				"marshal error",
				5,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
				marshalFail,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{nil},
			},
			{
				"total cache fail",
				20,
				1,
				[]cacheGetMultiFunc{
					cacheGetMultiFail,
					cacheGetMultiFail,
				},
				cacheAddMultiFail,
				cacheCompareAndSwapMultiFail,
				nil,
				nds.MarshalPropertyList,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{nil},
			},
			{
				"lock cache fail",
				20,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					cacheGetMultiFail,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
				nds.MarshalPropertyList,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{nil},
			},
			{
				"cache corrupt",
				2,
				2,
				[]cacheGetMultiFunc{
					// Charge cache.
					cacher.GetMulti,
					cacher.GetMulti,
					// Corrupt cache.
					func(c context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(c, keys)
						// Corrupt items.
						for _, item := range items {
							item.Value = []byte("corrupt string")
						}
						return items, err
					},
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
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
				"cache flag corrupt",
				2,
				2,
				[]cacheGetMultiFunc{
					// Charge cache.
					cacher.GetMulti,
					cacher.GetMulti,
					// Corrupt cache flags.
					func(c context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(c, keys)
						// Corrupt flags with unknown number.
						for _, item := range items {
							item.Flags = 56
						}
						return items, err
					},
					cacher.GetMulti,
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
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
				"lock cache value fail",
				20,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(c context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(c, keys)
						// Corrupt flags with unknown number.
						for _, item := range items {
							item.Value = []byte("corrupt value")
						}
						return items, err
					},
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
				nds.MarshalPropertyList,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{nil},
			},
			{
				"lock cache value none item",
				2,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(c context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(c, keys)
						// Corrupt flags with unknown number.
						for _, item := range items {
							item.Flags = nds.NoneItem
						}
						return items, err
					},
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
				nds.MarshalPropertyList,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{
					datastore.MultiError{
						datastore.ErrNoSuchEntity,
						datastore.ErrNoSuchEntity,
					},
				},
			},
			{
				"cache get no entity unmarshal fail",
				2,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(c context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(c, keys)
						// Corrupt flags with unknown number.
						for _, item := range items {
							item.Flags = nds.EntityItem
						}
						return items, err
					},
				},
				cacher.AddMulti,
				cacher.CompareAndSwapMulti,
				nil,
				nds.MarshalPropertyList,
				[]unmarshalFunc{
					nds.UnmarshalPropertyList,
					nds.UnmarshalPropertyList,
				},
				[]error{nil},
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ndsClient, err := NewClient(c, testCacher, t)
				if err != nil {
					t.Fatal(err)
				}

				keys, putVals := keysVals(c, test.keyCount)
				if _, err := ndsClient.PutMulti(c, keys, putVals); err != nil {
					t.Fatal(err)
				}

				cacheGetChan := make(chan cacheGetMultiFunc,
					len(test.cacheGetMultis))

				for _, fn := range test.cacheGetMultis {
					cacheGetChan <- fn
				}
				close(cacheGetChan)

				testCacher.getMultiHook = func(c context.Context, keys []string) (
					map[string]*nds.Item, error) {
					fn := <-cacheGetChan
					return fn(c, keys)
				}

				testCacher.addMultiHook = test.cacheAddMulti
				testCacher.compareAndSwapHook = test.cacheCompareAndSwapMulti

				nds.SetDatastoreGetMultiHook(test.datastoreGetMulti)

				nds.SetMarshal(test.marshal)

				unmarshalChan := make(chan unmarshalFunc,
					len(test.unmarshals))

				for _, fn := range test.unmarshals {
					unmarshalChan <- fn
				}
				close(unmarshalChan)

				nds.SetUnmarshal(func(data []byte, pl *datastore.PropertyList) error {
					fn := <-unmarshalChan
					return fn(data, pl)
				})

				for i := 0; i < test.callCount; i++ {
					getVals := make([]testEntity, test.keyCount)
					err := ndsClient.GetMulti(c, keys, getVals)

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
					expectedMultiErr, isMultiErr := expectedErr.(datastore.MultiError)

					if isMultiErr {
						me, ok := err.(datastore.MultiError)
						if !ok {
							t.Fatal("expected datastore.MultiError but got", err)
						}

						if len(me) != len(expectedMultiErr) {
							t.Fatal("datastore.MultiError length incorrect")
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
				testCacher.getMultiHook = nil
				testCacher.addMultiHook = nil
				testCacher.compareAndSwapHook = nil
				nds.SetDatastoreGetMultiHook(nil)
				nds.SetMarshal(nds.MarshalPropertyList)
				nds.SetUnmarshal(nds.UnmarshalPropertyList)

				if err := ndsClient.DeleteMulti(c, keys); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type loadSaveStruct struct {
	Value int64
}

func (lss *loadSaveStruct) Save() ([]datastore.Property, error) {
	return []datastore.Property{
		datastore.Property{
			Name:    "Val",
			Value:   lss.Value,
			NoIndex: true,
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

func PropertyLoadSaverTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{
			datastore.IncompleteKey("PropertyLoadSaverTest", nil),
		}
		entities := []*loadSaveStruct{
			&loadSaveStruct{
				Value: 23,
			},
		}
		keys, err = ndsClient.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatal(err)
		}

		entities = make([]*loadSaveStruct, 1)
		if err := ndsClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].Value != 23 {
			t.Fatal("expected another value")
		}
	}
}

func UnsupportedValueTypeTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{
			datastore.IncompleteKey("UnsupportedValueTypeTest", nil),
		}
		entities := make([]int, 1)
		if err := ndsClient.GetMulti(ctx, keys, entities); err == nil {
			t.Fatal("expected unsupported value error")
		}
	}
}

func GetMultiFieldMismatchTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		dsClient, err := datastore.NewClient(c, "")
		if err != nil {
			t.Fatal(err)
		}

		ndsClient, err := NewClient(c, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

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
			keys = append(keys, datastore.IDKey("GetMultiFieldMismatchTest", i, nil))
			entities = append(entities, testEntity{i, i})
		}

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore using nds.
		ndsResponse := make([]testEntityLean, len(keys))
		ndsErr := ndsClient.GetMulti(c, keys, ndsResponse)

		// Get from datastore using google api
		dsResponse := make([]testEntityLean, len(keys))
		dsErr := dsClient.GetMulti(c, keys, dsResponse)

		if ndsErr.Error() != dsErr.Error() {
			t.Fatal("Errors are not equal")
		}

		for i := int64(0); i < 2; i++ {
			if ndsResponse[i].IntVal != dsResponse[i].IntVal {
				t.Fatalf("IntVals are not equal %d %d", ndsResponse[i].IntVal, dsResponse[i].IntVal)
			}
		}
	}
}
