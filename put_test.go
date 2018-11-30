package nds_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds/v2"
)

func TestPutSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestPutMulti", PutMultiTest(item.ctx, item.cacher))
			t.Run("TestPutMultiError", PutMultiErrorTest(item.ctx, item.cacher))
			t.Run("TestPutMultiNoPropertyList", PutMultiNoPropertyListTest(item.ctx, item.cacher))
			t.Run("TestPutPropertyLoadSaver", PutPropertyLoadSaverTest(item.ctx, item.cacher))
			t.Run("TestPutNilArgs", PutNilArgsTest(item.ctx, item.cacher))
			t.Run("TestPutMultiLockFailure", PutMultiLockFailureTest(item.ctx, item.cacher))
			t.Run("TestPutMultiUnlockCacheSuccess", PutMultiUnlockCacheSuccessTest(item.ctx, item.cacher))
			t.Run("TestPutDatastoreMultiError", PutDatastoreMultiErrorTest(item.ctx, item.cacher))
			t.Run("TestPutMultiZeroKeys", PutMultiZeroKeysTest(item.ctx, item.cacher))
		})
	}
}

func PutMultiTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type TestEntity struct {
			Value int
		}

		for _, count := range []int{499, 500, 501} {
			keys := make([]*datastore.Key, count)
			entities := make([]TestEntity, count)

			for i := range keys {
				keys[i] = datastore.NameKey("TestEntity", strconv.Itoa(i), nil)
				entities[i] = TestEntity{i}
			}

			if _, err := ndsClient.PutMulti(ctx, keys, entities); err != nil {
				t.Fatal(err)
			}

			entities = make([]TestEntity, count)
			if err := ndsClient.GetMulti(ctx, keys, entities); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func PutMultiErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		expectedErrs := datastore.MultiError{
			nil,
			errors.New("expected error"),
		}

		nds.SetDatastorePutMultiHook(func() error {
			return expectedErrs
		})
		defer nds.SetDatastorePutMultiHook(nil)

		keys := []*datastore.Key{
			datastore.IDKey("Test", 1, nil),
			datastore.IDKey("Test", 2, nil),
		}

		type TestEntity struct {
			Value int
		}
		entities := []TestEntity{
			{1},
			{2},
		}

		_, err = ndsClient.PutMulti(ctx, keys, entities)
		me, ok := err.(datastore.MultiError)
		if !ok {
			t.Fatal("expected datastore.MultiError")
		}
		for i, e := range me {
			if e != expectedErrs[i] {
				t.Fatal("error incorrect")
			}
		}
	}
}

func PutMultiNoPropertyListTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("Test", 1, nil)}
		pl := datastore.PropertyList{datastore.Property{}}

		if _, err := ndsClient.PutMulti(ctx, keys, pl); err == nil {
			t.Fatal("expecting no PropertyList error")
		}
	}
}

func PutPropertyLoadSaverTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		te := &testEntity{2}
		pl, err := datastore.SaveStruct(te)
		if err != nil {
			t.Fatal(err)
		}

		keys := []*datastore.Key{datastore.IDKey("Test", 1, nil)}

		pls := datastore.PropertyList(pl)
		if _, err := ndsClient.PutMulti(ctx, keys,
			[]datastore.PropertyLoadSaver{&pls}); err != nil {
			t.Fatal(err)
		}

		getPl := datastore.PropertyList{}
		if err := ndsClient.GetMulti(ctx,
			keys, []datastore.PropertyLoadSaver{&getPl}); err != nil {
			t.Fatal(err)
		}
		getTe := &testEntity{}
		if err := datastore.LoadStruct(getTe, getPl); err != nil {
			t.Fatal(err)
		}
		if te.IntVal != getTe.IntVal {
			t.Fatal("expected same IntVal", getTe.IntVal)
		}
	}
}

func PutNilArgsTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ndsClient.Put(ctx, nil, nil); err == nil {
			t.Fatal("expected error")
		}
	}
}

func PutMultiLockFailureTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
			setMultiHook: func(_ context.Context, _ []*nds.Item) error {
				return errors.New("expected error")
			},
		}

		ndsClient, err := NewClient(ctx, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		keys := []*datastore.Key{datastore.IDKey("Test", 1, nil)}
		vals := []testEntity{{42}}

		if _, err := ndsClient.PutMulti(ctx, keys, vals); err == nil {
			t.Fatal("expected nds.PutMulti error")
		}
	}
}

// Make sure PutMulti still works if we have a cache unlock failure.
func PutMultiUnlockCacheSuccessTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
			deleteMultiHook: func(_ context.Context, _ []string) error {
				return errors.New("expected error")
			},
		}

		ndsClient, err := NewClient(ctx, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		keys := []*datastore.Key{datastore.IDKey("Test", 1, nil)}
		vals := []testEntity{{42}}

		if _, err := ndsClient.PutMulti(ctx, keys, vals); err != nil {
			t.Fatal(err)
		}
	}
}

func PutDatastoreMultiErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		expectedErr := errors.New("expected error")

		testCacher := &mockCacher{
			cacher: cacher,
		}

		nds.SetDatastorePutMultiHook(func() error {
			return datastore.MultiError{expectedErr}
		})
		defer nds.SetDatastorePutMultiHook(nil)

		ndsClient, err := NewClient(ctx, testCacher, t)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		key := datastore.IDKey("Test", 1, nil)
		val := &testEntity{42}

		if _, err := ndsClient.Put(ctx, key, val); err == nil {
			t.Fatal("expected error")
		} else if err != expectedErr {
			t.Fatal("should be expectedErr")
		}
	}
}

func PutMultiZeroKeysTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := ndsClient.PutMulti(ctx, []*datastore.Key{},
			[]interface{}{}); err != nil {
			t.Fatal(err)
		}
	}
}
