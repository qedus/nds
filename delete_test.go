package nds_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds"
)

func TestDeleteSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("DeleteMultiTest", DeleteMultiTest(item.ctx, item.cacher))
			//t.Run("DeleteNilKeyTest", DeleteNilKeyTest(item.ctx, item.cacher))
			t.Run("DeleteIncompleteKeyTest", DeleteIncompleteKeyTest(item.ctx, item.cacher))
			t.Run("DeleteCacheFailTest", DeleteCacheFailTest(item.ctx, item.cacher))
			t.Run("DeleteInTransactionTest", DeleteInTransactionTest(item.ctx, item.cacher))
		})
	}
}

func DeleteMultiTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher)
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

			if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
				t.Fatal(err)
			}

			// Prime cache.
			entities = make([]TestEntity, count)
			if err := ndsClient.GetMulti(c, keys, entities); err != nil {
				t.Fatal(err)
			}

			if err = ndsClient.DeleteMulti(c, keys); err != nil {
				t.Fatal(err)
			}

			err = ndsClient.GetMulti(c, keys, make([]TestEntity, count))
			if err == nil {
				t.Fatal("expect error")
			}

			me, ok := err.(datastore.MultiError)
			if !ok {
				t.Fatal("should be MultiError")
			}

			for _, e := range me {
				if e != datastore.ErrNoSuchEntity {
					t.Fatal("expected ErrNoSuchEntity")
				}
			}
		}
	}
}

// Test will panic, see https://github.com/GoogleCloudPlatform/google-cloud-go/issues/1175
// func DeleteNilKeyTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
// 	return func(t *testing.T) {
// 		ndsClient, err := NewClient(c, cacher)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		if err := ndsClient.Delete(c, nil); err != datastore.ErrInvalidKey {
// 			t.Fatal("expected nil key error")
// 		}
// 	}
// }

func DeleteIncompleteKeyTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher)
		if err != nil {
			t.Fatal(err)
		}

		key := datastore.IncompleteKey("Entity", nil)

		if err := ndsClient.Delete(c, key); err == nil {
			t.Fatal("expected invalid key error")
		}
	}
}

func DeleteCacheFailTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(c, testCacher)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("Entity", 1, nil)
		keys := []*datastore.Key{key}
		entities := make([]testEntity, 1)
		entities[0].Val = 43

		if _, err := ndsClient.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		testCacher.setMultiHook = func(_ context.Context, _ []*nds.Item) error {
			return errors.New("expected error")
		}

		if err := ndsClient.DeleteMulti(c, keys); err == nil {
			t.Fatal("expected DeleteMulti error")
		}
	}
}

func DeleteInTransactionTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("TestEntity", 1, nil)
		if _, err := ndsClient.Put(c, key, &testEntity{2}); err != nil {
			t.Fatal(err)
		}

		// Prime cache.
		if err := ndsClient.Get(c, key, &testEntity{}); err != nil {
			t.Fatal(err)
		}

		if _, err = ndsClient.RunInTransaction(c, func(tx *nds.Transaction) error {
			return tx.DeleteMulti([]*datastore.Key{key})
		}); err != nil {
			t.Fatal(err)
		}

		if err = ndsClient.Get(c, key, &testEntity{}); err == nil {
			t.Fatal("expected no entity")
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}
	}
}
