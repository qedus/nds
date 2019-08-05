package nds_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds/v2"
)

func TestGetSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestGetMultiStruct", GetMultiStructTest(item.ctx, item.cacher))
			t.Run("TestGetMultiStructPtr", GetMultiStructPtrTest(item.ctx, item.cacher))
			t.Run("TestGetMultiStructPtrNil", GetMultiStructPtrNilTest(item.ctx, item.cacher))
			t.Run("TestGetMultiInterface", GetMultiInterfaceTest(item.ctx, item.cacher))
			t.Run("TestGetMultiPropertyLoadSaver", GetMultiPropertyLoadSaverTest(item.ctx, item.cacher))
			t.Run("TestGetMultiKeyLoader", GetMultiKeyLoaderTest(item.ctx, item.cacher))
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
			t.Run("TestGetMultiLockDatastoreUnknownError", GetMultiLockDatastoreUnknownErrorTest(item.ctx, item.cacher))
			t.Run("TestGetNamespacedKey", GetNamespacedKeyTest(item.ctx, item.cacher))
			t.Run("TestGetMultiPaths", GetMultiPathsTest(item.ctx, item.cacher))
			t.Run("TestPropertyLoadSaver", PropertyLoadSaverTest(item.ctx, item.cacher))
			t.Run("TestKeyLoader", KeyLoaderTest(item.ctx, item.cacher))
			t.Run("TestUnsupportedValueType", UnsupportedValueTypeTest(item.ctx, item.cacher))
			t.Run("TestGetMultiFieldMismatch", GetMultiFieldMismatchTest(item.ctx, item.cacher))
			t.Run("TestGetMultiExpiredContext", GetMultiExpiredContextTest(item.ctx, item.cacher))
			t.Run("TestPropertyLoadSaverModification", PropertyLoadSaverModificationTest(item.ctx, item.cacher))
		})
	}
}

func GetMultiStructTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}

		// Get from cache.
		response = make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiStructPtrTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]*testEntity, len(keys))
		for i := 0; i < len(response); i++ {
			response[i] = &testEntity{}
		}

		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiStructPtrNilTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]*testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}

		// Get from cache.
		response = make([]*testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 2; i++ {
			if response[i].IntVal != i+1 {
				t.Fatal("incorrect IntVal")
			}
		}
	}
}

func GetMultiInterfaceTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		response := make([]interface{}, len(keys))
		for i := 0; i < len(response); i++ {
			response[i] = &testEntity{}
		}

		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
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

func GetMultiPropertyLoadSaverTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Prime the cache.
		uncachedEntities := make([]datastore.PropertyList, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, uncachedEntities); err != nil {
			t.Fatal(err)
		}

		for i, e := range entities {
			if !reflect.DeepEqual(e, uncachedEntities[i]) {
				t.Fatal("uncachedEntities not equal", e, uncachedEntities[i])
			}
		}

		// Use cache.
		cachedEntities := make([]datastore.PropertyList, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, cachedEntities); err != nil {
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
		nds.SetDatastoreGetMultiHook(func(ctx context.Context,
			keys []*datastore.Key, vals interface{}) error {
			if len(keys) != 0 {
				return errors.New("should not be called")
			}
			return nil
		})
		defer nds.SetDatastoreGetMultiHook(nil)

		tes := make([]testEntity, len(entities))
		if err := ndsClient.GetMulti(ctx, keys, tes); err != nil {
			t.Fatal(err)
		}
	}
}

type keyLoaderTest struct {
	I    int64
	Key3 int64
}

func (k *keyLoaderTest) Load(pl []datastore.Property) error {
	return datastore.LoadStruct(k, pl)
}

func (k *keyLoaderTest) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(k)
}

func (k *keyLoaderTest) LoadKey(key *datastore.Key) error {
	if key == nil {
		return fmt.Errorf("got nil key!")
	}

	k.Key3 = key.ID * 3

	return nil
}

var (
	_ datastore.KeyLoader = (*keyLoaderTest)(nil)
)

func GetMultiKeyLoaderTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{}
		entities := []keyLoaderTest{}

		for i := 1; i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiKeyLoaderTest", int64(i), nil))

			entities = append(entities, keyLoaderTest{I: int64(i)})
		}

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Update entities to reflect KeyLoader process
		for i := range entities {
			entities[i].Key3 = keys[i].ID * 3
		}

		// Prime the cache.
		uncachedEntities := make([]keyLoaderTest, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, uncachedEntities); err != nil {
			t.Fatal(err)
		}

		for i, e := range entities {
			if !reflect.DeepEqual(e, uncachedEntities[i]) {
				t.Fatal("uncachedEntities not equal", e, uncachedEntities[i])
			}
		}

		// Use cache.
		cachedEntities := make([]keyLoaderTest, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, cachedEntities); err != nil {
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
		nds.SetDatastoreGetMultiHook(func(ctx context.Context,
			keys []*datastore.Key, vals interface{}) error {
			if len(keys) != 0 {
				return errors.New("should not be called")
			}
			return nil
		})
		defer nds.SetDatastoreGetMultiHook(nil)

		tes := make([]keyLoaderTest, len(entities))
		if err := ndsClient.GetMulti(ctx, keys, tes); err != nil {
			t.Fatal(err)
		}
	}
}

func GetMultiNoKeysTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}

		if err := ndsClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}
	}
}

func GetMultiInterfaceErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, func(err error) bool {
			return strings.Contains(err.Error(), "invalid entity type")
		})

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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		// No errors expected.
		response := []interface{}{&testEntity{}, &testEntity{}}

		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
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
		if err := ndsClient.GetMulti(ctx, keys, response); err == nil {
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

func GetArgsTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		if err := ndsClient.Get(ctx, nil, &testEntity{}); err == nil {
			t.Fatal("expected error for nil key")
		}

		key := datastore.IDKey("GetArgsTest", 1, nil)
		if err := ndsClient.Get(ctx, key, nil); err != datastore.ErrInvalidEntityType {
			t.Fatal("expected ErrInvalidEntityType for nil value")
		}

		if err := ndsClient.Get(ctx, key, datastore.PropertyList{}); err == nil {
			t.Fatal("expected error for datastore.PropertyList")
		}

		if err := ndsClient.Get(ctx, key, testEntity{}); err == nil {
			t.Fatal("expected error for struct")
		}

		rte := newReaderTestEntity()
		if err := ndsClient.Get(ctx, key, rte); err == nil {
			t.Fatal("expected error for interface")
		}
	}
}

func GetMultiArgsTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		key := datastore.IDKey("GetMultiArgsTest", 1, nil)
		keys := []*datastore.Key{key}
		val := testEntity{}
		if err := ndsClient.GetMulti(ctx, keys, nil); err == nil {
			t.Fatal("expected error for nil vals")
		}
		structVals := []testEntity{val}
		if err := ndsClient.GetMulti(ctx, nil, structVals); err == nil {
			t.Fatal("expected error for nil keys")
		}

		if err := ndsClient.GetMulti(ctx, keys, []testEntity{}); err == nil {
			t.Fatal("expected error for unequal keys and vals")
		}

		if err := ndsClient.GetMulti(ctx, keys, datastore.PropertyList{}); err == nil {
			t.Fatal("expected error for propertyList")
		}

		rte := newReaderTestEntity()
		if err := ndsClient.GetMulti(ctx, keys, []io.Reader{rte}); err == nil {
			t.Fatal("expected error for interface")
		}
	}
}

func GetSlicePropertyTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVals []int64
		}

		key := datastore.IDKey("GetSlicePropertyTest", 1, nil)
		intVals := []int64{0, 1, 2, 3}
		val := &testEntity{intVals}

		if _, err := ndsClient.Put(ctx, key, val); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		newVal := &testEntity{}
		if err := ndsClient.Get(ctx, key, newVal); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(newVal.IntVals, intVals) {
			t.Fatal("slice properties not equal", newVal.IntVals)
		}

		// Get from cache.
		newVal = &testEntity{}
		if err := ndsClient.Get(ctx, key, newVal); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(newVal.IntVals, intVals) {
			t.Fatal("slice properties not equal", newVal.IntVals)
		}
	}
}

func GetMultiNoPropertyListTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("GetMultiNoPropertyListTest", 1, nil)}
		pl := datastore.PropertyList{datastore.Property{}}

		if err := ndsClient.GetMulti(ctx, keys, pl); err == nil {
			t.Fatal("expecting no PropertyList error")
		}
	}
}

func GetMultiNonStructTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("GetMultiNonStructTest", 1, nil)}
		vals := []int{12}

		if err := ndsClient.GetMulti(ctx, keys, vals); err == nil {
			t.Fatal("expecting unsupported vals type")
		}
	}
}

func GetMultiLockReturnEntitySetValueFailTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, func(err error) bool {
			return strings.Contains(err.Error(), "cannot load field \"One\"")
		})
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Fail to unmarshal test.
		cacheGetChan := make(chan func(ctx context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(ctx, keys)
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

		testCacher.getMultiHook = func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(ctx, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnEntityTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(ctx context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(ctx, keys)
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
		testCacher.getMultiHook = func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(ctx, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if 5 != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnUnknownTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, func(err error) bool {
			return strings.Contains(err.Error(), "lockCache unknown")
		})
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(ctx context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(ctx, keys)
			if err != nil {
				return nil, err
			}

			// Unknown lock values.
			items[keys[0]].Flags = 23
			items[keys[1]].Flags = 24
			return items, nil
		}
		testCacher.getMultiHook = func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(ctx, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockReturnMissTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		cacheGetChan := make(chan func(ctx context.Context, keys []string) (
			map[string]*nds.Item, error), 2)
		cacheGetChan <- cacher.GetMulti
		cacheGetChan <- func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			items, err := cacher.GetMulti(ctx, keys)
			if err != nil {
				return nil, err
			}

			// Remove one item between the cache Add and Get.
			delete(items, keys[0])
			return items, nil
		}
		testCacher.getMultiHook = func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			f := <-cacheGetChan
			return f(ctx, keys)
		}

		response := make([]testEntity, len(keys))
		if err := ndsClient.GetMulti(ctx, keys, response); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(keys); i++ {
			if entities[i].IntVal != response[i].IntVal {
				t.Fatal("IntVal not equal")
			}
		}
	}
}

func GetMultiLockDatastoreUnknownErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := int64(1); i < 3; i++ {
			keys = append(keys, datastore.IDKey("GetMultiLockReturnErrorTest", i, nil))
			entities = append(entities, testEntity{i})
		}

		if _, err = ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		invalidKey := datastore.NameKey("GetMultiLockReturnErrorTest", "invalid", nil)
		invalidKey.ID = 9000 // Key should name a Name or ID, but not both
		keys = append(keys, invalidKey)
		invalidKeyCalled := false
		invalidCacheKey := nds.CreateCacheKey(invalidKey)
		testCacher.compareAndSwapHook = func(ctx context.Context,
			items []*nds.Item) error {
			// shouldn't find invalid last item here
			for _, item := range items {
				if item.Key == invalidCacheKey {
					invalidKeyCalled = true
					return errors.New("should not have called this")
				}
			}
			return cacher.CompareAndSwapMulti(ctx, items)
		}

		response := make([]testEntity, len(keys))
		if err = ndsClient.GetMulti(ctx, keys, response); err == nil {
			t.Error("expected non-nil err")
		}

		if invalidKeyCalled {
			t.Fatal("Invalid key was not skipped")
		}

		if me, ok := err.(datastore.MultiError); !ok {
			t.Errorf("expected MultiError, got %v", err)
		} else {
			for i := 0; i < len(keys)-1; i++ {
				if me[i] != nil {
					t.Errorf("expected err=nil, got %v", me[i])
				}
			}
			if got := me[len(keys)-1]; got != datastore.ErrInvalidKey {
				t.Errorf("expected ErrInvalidKey, got %d", got)
			}
		}
	}
}

// TestGetNamespacedKey ensures issue https://goo.gl/rXU8nK is fixed so that
// the cache uses the namespace from the key instead of the context.
func GetNamespacedKeyTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if namespacedKey, err = ndsClient.Put(ctx, namespacedKey, entity); err != nil {
			t.Fatal(err)
		}

		// Prime cache.
		if err := ndsClient.Get(ctx, namespacedKey, &testEntity{}); err != nil {
			t.Fatal(err)
		}

		// Ensure that we get a value back from the cache by checking if the
		// datastore is called at all.
		entityFromCache := true
		nds.SetDatastoreGetMultiHook(func(ctx context.Context,
			keys []*datastore.Key, vals interface{}) error {
			if len(keys) != 0 {
				entityFromCache = false
			}
			return nil
		})
		if err := ndsClient.Get(ctx, namespacedKey, &testEntity{}); err != nil {
			t.Fatal(err)
		}
		nds.SetDatastoreGetMultiHook(nil)

		if !entityFromCache {
			t.Fatal("entity not obtained from cache")
		}

		if err := ndsClient.Delete(ctx, namespacedKey); err != nil {
			t.Fatal(err)
		}

		entity = &testEntity{}
		if err := ndsClient.Get(ctx, namespacedKey, entity); err == nil {
			t.Fatalf("expected no such entity error but got %+v", entity)
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}
	}
}

func GetMultiPathsTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}

		expectedErr := errors.New("expected error")

		type cacheGetMultiFunc func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error)
		cacheGetMultiFail := func(ctx context.Context,
			keys []string) (map[string]*nds.Item, error) {
			return nil, expectedErr
		}

		type cacheAddMultiFunc func(ctx context.Context,
			items []*nds.Item) error
		cacheAddMultiFail := func(ctx context.Context,
			items []*nds.Item) error {
			return expectedErr
		}

		type cacheCompareAndSwapMultiFunc func(ctx context.Context,
			items []*nds.Item) error
		cacheCompareAndSwapMultiFail := func(ctx context.Context,
			items []*nds.Item) error {
			return expectedErr
		}

		type datastoreGetMultiFunc func(ctx context.Context,
			keys []*datastore.Key, vals interface{}) error
		datastoreGetMultiFail := func(ctx context.Context,
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

		keysVals := func(ctx context.Context, count int64) (
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
			logTester    func(error) bool
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
				nil,
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
				nil,
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
				func(ctx context.Context,
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
				nil,
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
				func(err error) bool { return strings.Contains(err.Error(), expectedErr.Error()) },
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
				func(err error) bool { return strings.Contains(err.Error(), expectedErr.Error()) },
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
				func(err error) bool { return strings.Contains(err.Error(), expectedErr.Error()) },
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
					func(ctx context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(ctx, keys)
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
				func(err error) bool { return strings.Contains(err.Error(), "unexpected EOF") },
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
					func(ctx context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(ctx, keys)
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
				func(err error) bool { return strings.Contains(err.Error(), "loadCache unknown") },
			},
			{
				"lock cache value fail",
				20,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(ctx context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(ctx, keys)
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
				func(err error) bool { return strings.Contains(err.Error(), expectedErr.Error()) },
			},
			{
				"lock cache value none item",
				2,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(ctx context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(ctx, keys)
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
				nil,
			},
			{
				"cache get no entity unmarshal fail",
				2,
				1,
				[]cacheGetMultiFunc{
					cacher.GetMulti,
					func(ctx context.Context, keys []string) (
						map[string]*nds.Item, error) {
						items, err := cacher.GetMulti(ctx, keys)
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
				func(err error) bool { return strings.Contains(err.Error(), "lockCache unmarshal") },
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ndsClient, err := NewClient(ctx, testCacher, t, test.logTester)
				if err != nil {
					t.Fatal(err)
				}

				keys, putVals := keysVals(ctx, test.keyCount)
				if _, err := ndsClient.PutMulti(ctx, keys, putVals); err != nil {
					t.Fatal(err)
				}

				cacheGetChan := make(chan cacheGetMultiFunc,
					len(test.cacheGetMultis))

				for _, fn := range test.cacheGetMultis {
					cacheGetChan <- fn
				}
				close(cacheGetChan)

				testCacher.getMultiHook = func(ctx context.Context, keys []string) (
					map[string]*nds.Item, error) {
					fn := <-cacheGetChan
					return fn(ctx, keys)
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
					err := ndsClient.GetMulti(ctx, keys, getVals)

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

				if err := ndsClient.DeleteMulti(ctx, keys); err != nil {
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
			if p.Value.(int64) < 0 {
				return fmt.Errorf("negative value")
			}
			lss.Value = p.Value.(int64)
		}
	}
	return nil
}

func PropertyLoadSaverTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{
			datastore.IncompleteKey("PropertyLoadSaverTest", nil),
			datastore.IncompleteKey("PropertyLoadSaverTest", nil),
		}
		entities := []*loadSaveStruct{
			&loadSaveStruct{
				Value: 23,
			},
			&loadSaveStruct{
				Value: -1,
			},
		}
		keys, err = ndsClient.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatal(err)
		}

		entities = make([]*loadSaveStruct, 1)
		if err := ndsClient.GetMulti(ctx, keys[:1], entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].Value != 23 {
			t.Fatal("expected another value")
		}

		if err := ndsClient.GetMulti(ctx, keys[1:], entities); err == nil {
			t.Fatal("expected error")
		}
	}
}

func KeyLoaderTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{
			datastore.IncompleteKey("KeyLoaderTest", nil),
		}
		entities := []*keyLoaderTest{
			&keyLoaderTest{
				I: 23,
			},
		}
		keys, err = ndsClient.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatal(err)
		}

		entities = make([]*keyLoaderTest, 1)
		if err := ndsClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].I != 23 || entities[0].Key3 != keys[0].ID*3 {
			t.Fatal("expected another value")
		}
	}
}

func UnsupportedValueTypeTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

func GetMultiFieldMismatchTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		dsClient, err := datastore.NewClient(ctx, "")
		if err != nil {
			t.Fatal(err)
		}

		ndsClient, err := NewClient(ctx, cacher, t, nil)
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

		if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from datastore using nds.
		ndsResponse := make([]testEntityLean, len(keys))
		ndsErr := ndsClient.GetMulti(ctx, keys, ndsResponse)

		// Get from datastore using google api
		dsResponse := make([]testEntityLean, len(keys))
		dsErr := dsClient.GetMulti(ctx, keys, dsResponse)

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

func GetMultiExpiredContextTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		nds.SetDatastoreGetMultiHook(func(ctx context.Context,
			keys []*datastore.Key, vals interface{}) error {
			cancel()
			return nil
		})
		defer nds.SetDatastoreGetMultiHook(nil)

		type testEntity struct {
			IntVal int64
		}

		keys := []*datastore.Key{
			datastore.IDKey("GetMultiErrTest", 23, nil),
		}

		if _, err := ndsClient.Put(ctx, keys[0], &testEntity{1}); err != nil {
			t.Fatalf("got err %v", err)
		}

		entities := make([]testEntity, 1)
		if err := ndsClient.GetMulti(cctx, keys, entities); err == nil {
			t.Errorf("expected non-nil error")
		} else if !strings.Contains(err.Error(), cctx.Err().Error()) {
			t.Errorf("expected err `%v` to contain `%v`", cctx.Err(), err)
		}
	}
}

type PropertyLoadSaverModifyTest struct {
	Name  string
	Name2 string
	Name3 string
	Name4 string
}

func (p *PropertyLoadSaverModifyTest) Load(ps []datastore.Property) error {
	// Drop the second item
	ps = append(ps[:1], ps[2:]...)

	return datastore.LoadStruct(p, ps)
}

func (p *PropertyLoadSaverModifyTest) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(p)
}

// PropertyLoadSaverModificationTest tests for the situation in which the PropertyList is shrunk in the Load call
// but nds is unaware
func PropertyLoadSaverModificationTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		key := datastore.NameKey("PropertyLoadSaverModifyTest", "name", nil)
		val := &PropertyLoadSaverModifyTest{
			Name:  "Name",
			Name2: "Name2",
			Name3: "Name3",
			Name4: "Name4",
		}

		if _, err := ndsClient.Put(ctx, key, val); err != nil {
			t.Fatal(err)
		}

		// Get from datastore.
		newVal := &PropertyLoadSaverModifyTest{}
		if err := ndsClient.Get(ctx, key, newVal); err != nil {
			t.Fatal(err)
		}

		// Get from cache.
		newVal = &PropertyLoadSaverModifyTest{}
		if err := ndsClient.Get(ctx, key, newVal); err != nil {
			t.Fatal(err)
		}
	}
}
