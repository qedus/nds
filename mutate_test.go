package nds_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds/v2"
)

func TestMutate(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestMutateInsert", MutateInsertTests(item.ctx, item.cacher))
			t.Run("TestMutateUpsert", MutateUpsertTests(item.ctx, item.cacher))
			t.Run("TestMutateUpdate", MutateUpdateTests(item.ctx, item.cacher))
			t.Run("TestMutateDelete", MutateDeleteTests(item.ctx, item.cacher))
			t.Run("TestMutateZeroArg", MutateZeroArgTest(item.ctx, item.cacher))
			t.Run("TestMutateLockFailure", MutateLockFailureTest(item.ctx, item.cacher))
			t.Run("TestMutateUnlockCacheSuccess", MutateUnlockCacheSuccessTest(item.ctx, item.cacher))
			t.Run("TestMutateDatastoreError", MutateDatastoreErrorTest(item.ctx, item.cacher))
			t.Run("TestMutateBadContext", MutateBadContextTest(item.ctx, item.cacher))
			t.Run("TestMutateTracking", MutateTrackingTest(item.ctx, item.cacher))
		})
	}
}

func MutateInsertTests(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int64
		}

		var keys []*datastore.Key

		t.Run("Success", func(t *testing.T) {
			// Basic Insert
			entity := &testEntity{64}
			keys, err = ndsClient.Mutate(ctx, nds.NewInsert(datastore.IncompleteKey("MutateInsertTests", nil), entity))
			if err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if dest.Value != entity.Value {
				t.Fatalf("expected dest.Value=%d, got %d", entity.Value, dest.Value)
			}
		})

		if len(keys) != 1 {
			t.Fatalf("expected a single key, got %v", keys)
		}

		t.Run("Failure", func(t *testing.T) {
			// Insert duplicate failure
			entity := &testEntity{64}
			_, err = ndsClient.Mutate(ctx, nds.NewInsert(keys[0], &testEntity{65}))
			if err == nil {
				t.Fatal("expected err != nil, got nil")
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if dest.Value != entity.Value {
				t.Fatalf("expected dest.Value=%d, got %d", entity.Value, dest.Value)
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			// Multi Insert
			entity := &testEntity{99}
			inserts := []*nds.Mutation{
				nds.NewInsert(datastore.IncompleteKey("MutateInsertTests", nil), entity),
				nds.NewInsert(datastore.IncompleteKey("MutateInsertTests", nil), entity),
			}
			if keys, err = ndsClient.Mutate(ctx, inserts...); err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}

			dest := make([]testEntity, 2)
			if err = ndsClient.GetMulti(ctx, keys, dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if len(dest) != 2 || dest[0].Value != entity.Value || dest[1].Value != entity.Value {
				t.Fatalf("expected dest = {{99}, {99}}, got %v", dest)
			}
		})

		_ = ndsClient.DeleteMulti(ctx, keys) // Cleanup
	}
}

func MutateUpdateTests(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int64
		}

		keys := []*datastore.Key{
			datastore.NameKey("MutateUpdateTests", "test1", nil),
			datastore.NameKey("MutateUpdateTests", "test2", nil),
		}

		defer func() { _ = ndsClient.DeleteMulti(ctx, keys) }() // Cleanup

		t.Run("Failure", func(t *testing.T) {
			// Update non-existent key failure
			entity := &testEntity{64}
			_, err = ndsClient.Mutate(ctx, nds.NewUpdate(keys[0], entity))
			if err == nil {
				t.Fatal("expected err != nil, got nil")
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err == nil {
				t.Fatalf("unexpectedly got entity: %v", err)
			}
		})

		if _, err = ndsClient.PutMulti(ctx, keys, []testEntity{testEntity{123}, testEntity{123}}); err != nil {
			t.Fatalf("could not put into datastore: %v", err)
		}

		t.Run("Success", func(t *testing.T) {
			// Basic Updates
			entity := &testEntity{64}
			_, err = ndsClient.Mutate(ctx, nds.NewUpdate(keys[0], entity))
			if err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if dest.Value != entity.Value {
				t.Fatalf("expected dest.Value=%d, got %d", entity.Value, dest.Value)
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			// Multi Update
			entity := &testEntity{99}
			inserts := []*nds.Mutation{
				nds.NewUpdate(keys[0], entity),
				nds.NewUpdate(keys[1], entity),
			}
			if _, err = ndsClient.Mutate(ctx, inserts...); err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}

			dest := make([]testEntity, 2)
			if err = ndsClient.GetMulti(ctx, keys, dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if len(dest) != 2 || dest[0].Value != entity.Value || dest[1].Value != entity.Value {
				t.Fatalf("expected dest = {{99}, {99}}, got %v", dest)
			}
		})
	}
}

func MutateUpsertTests(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int64
		}

		var keys []*datastore.Key

		t.Run("Success", func(t *testing.T) {
			// Basic Upsert
			entity := &testEntity{64}
			keys, err = ndsClient.Mutate(ctx, nds.NewUpsert(datastore.IncompleteKey("MutateUpsertTests", nil), entity))
			if err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if dest.Value != entity.Value {
				t.Fatalf("expected dest.Value=%d, got %d", entity.Value, dest.Value)
			}
		})

		if len(keys) != 1 {
			t.Fatalf("expected a single key, got %v", keys)
		}

		t.Run("Multiple", func(t *testing.T) {
			// Multi Upsert, one update, one insert
			entity := &testEntity{99}
			upserts := []*nds.Mutation{
				nds.NewUpsert(keys[0], entity),
				nds.NewUpsert(datastore.IncompleteKey("MutateUpsertTests", nil), entity),
			}
			if keys, err = ndsClient.Mutate(ctx, upserts...); err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}

			dest := make([]testEntity, 2)
			if err = ndsClient.GetMulti(ctx, keys, dest); err != nil {
				t.Fatalf("could not get entity: %v", err)
			}
			if len(dest) != 2 || dest[0].Value != entity.Value || dest[1].Value != entity.Value {
				t.Fatalf("expected dest = {{99}, {99}}, got %v", dest)
			}
		})

		_ = ndsClient.DeleteMulti(ctx, keys) // Cleanup
	}
}

func MutateDeleteTests(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int64
		}

		keys := []*datastore.Key{
			datastore.NameKey("MutateDeleteTests", "test1", nil),
			datastore.NameKey("MutateDeleteTests", "test2", nil),
			datastore.NameKey("MutateDeleteTests", "test3", nil),
		}

		if _, err = ndsClient.PutMulti(ctx, keys, []testEntity{testEntity{123}, testEntity{123}, testEntity{123}}); err != nil {
			t.Fatalf("could not put into datastore: %v", err)
		}

		t.Run("Success", func(t *testing.T) {
			// Basic Delete
			_, err = ndsClient.Mutate(ctx, nds.NewDelete(keys[0]))
			if err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}
			var dest testEntity
			if err = ndsClient.Get(ctx, keys[0], &dest); err == nil {
				t.Fatalf("unexpectedly go entity: %v", dest)
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			// Multiple Deletes
			deletes := []*nds.Mutation{
				nds.NewDelete(keys[1]),
				nds.NewDelete(keys[2]),
			}
			if _, err = ndsClient.Mutate(ctx, deletes...); err != nil {
				t.Fatalf("expected err=nil, got %v", err)
			}

			dest := make([]testEntity, 2)
			if err = ndsClient.GetMulti(ctx, keys[1:], dest); err == nil {
				t.Fatalf("unexpectedly got entities: %v", dest)
			}
		})
	}
}

func MutateZeroArgTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ndsClient.Mutate(ctx); err != nil {
			t.Errorf("expected err = nil, got %v", err)
		}
	}
}

func MutateLockFailureTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		datastoreCalled := false

		testCacher := &mockCacher{
			cacher: cacher,
			setMultiHook: func(_ context.Context, _ []*nds.Item) error {
				return errors.New("expected error")
			},
		}

		nds.SetDatastoreMutateHook(func() error {
			datastoreCalled = true
			return nil
		})

		ndsClient, err := NewClient(ctx, testCacher, t, func(err error) bool {
			return strings.Contains(err.Error(), "Mutate cache.DeleteMulti")
		})
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		key := datastore.IDKey("MutateLockFailureTest", 1, nil)
		val := testEntity{42}

		if _, err := ndsClient.Mutate(ctx, nds.NewUpsert(key, &val)); err == nil {
			t.Fatal("expected nds.PutMulti error")
		}
		if datastoreCalled {
			t.Error("Datastore was called and should not have been!")
		}
	}
}

// Make sure Mutate still works if we have a cache unlock failure.
func MutateUnlockCacheSuccessTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
			deleteMultiHook: func(_ context.Context, _ []string) error {
				return errors.New("expected error")
			},
		}

		ndsClient, err := NewClient(ctx, testCacher, t, func(err error) bool {
			return strings.Contains(err.Error(), "expected error")
		})
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		key := datastore.IDKey("MutateUnlockCacheSuccessTest", 1, nil)
		val := testEntity{42}

		if _, err := ndsClient.Mutate(ctx, nds.NewUpsert(key, &val)); err != nil {
			t.Fatal(err)
		}
	}
}

func MutateDatastoreErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		expectedErr := errors.New("expected error")

		testCacher := &mockCacher{
			cacher: cacher,
		}

		nds.SetDatastoreMutateHook(func() error {
			return expectedErr
		})
		defer nds.SetDatastoreMutateHook(nil)

		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		key := datastore.IDKey("MutateDatastoreErrorTest", 1, nil)
		val := testEntity{42}

		if _, err := ndsClient.Mutate(ctx, nds.NewUpsert(key, &val)); err != expectedErr {
			t.Errorf("expected error=%v, got %v", expectedErr, err)
		}
	}
}

func MutateBadContextTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		badctx, cancel := context.WithCancel(ctx)
		cancel()

		ndsClient, err := NewClient(ctx, testCacher, t, func(err error) bool {
			return strings.Contains(err.Error(), context.Canceled.Error())
		})
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			IntVal int
		}

		key := datastore.IDKey("MutateDatastoreErrorTest", 1, nil)
		val := testEntity{42}

		if _, err := ndsClient.Mutate(badctx, nds.NewUpsert(key, &val)); err != badctx.Err() {
			t.Fatalf("expected err=%v, got %v", badctx.Err(), err)
		}
	}
}

func MutateTrackingTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	// Make sure we properly track cache lock items and releases, in the proper order
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}
		testKeys := []*datastore.Key{
			datastore.NameKey("MutateTrackingTest", "insertMutate", nil),
			datastore.NameKey("MutateTrackingTest", "upsertMutate", nil),
			datastore.NameKey("MutateTrackingTest", "updateMutate", nil),
			datastore.NameKey("MutateTrackingTest", "deleteMutate", nil),
		}
		expectedKeys := make([]string, len(testKeys))
		for i, key := range testKeys {
			expectedKeys[i] = nds.CreateCacheKey(key)
		}
		type testEntity struct {
			Value int64
		}

		setOk := false
		deleteOk := false
		mutateOk := false

		testCacher.setMultiHook = func(ctx context.Context, items []*nds.Item) error {
			if deleteOk || mutateOk || setOk {
				return fmt.Errorf("set multi should have been called first and only once!")
			}
			if got := len(items); got != len(expectedKeys) {
				return fmt.Errorf("expected len(keys) = %d, wanted %d", got, len(expectedKeys))
			}
			for i := range items {
				if items[i].Key != expectedKeys[i] {
					return fmt.Errorf("expected key = %s, got %s in position %d", expectedKeys[i], items[i].Key, i)
				}
			}
			setOk = true
			return cacher.SetMulti(ctx, items)
		}

		testCacher.deleteMultiHook = func(ctx context.Context, keys []string) error {
			if deleteOk || !mutateOk || !setOk {
				return fmt.Errorf("delete multi should have been called third and only once!")
			}
			if got := len(keys); got != len(expectedKeys)-1 {
				return fmt.Errorf("expected len(keys) = %d, wanted %d", got, len(expectedKeys))
			}
			for i := range expectedKeys[:len(expectedKeys)-2] {
				if keys[i] != expectedKeys[i] {
					return fmt.Errorf("expected key = %s, got %s in position %d", expectedKeys[i], keys[i], i)
				}
			}
			deleteOk = true
			return cacher.DeleteMulti(ctx, keys)
		}

		nds.SetDatastoreMutateHook(func() error {
			if deleteOk || mutateOk || !setOk {
				return fmt.Errorf("mutate should have been called second and only once!")
			}
			mutateOk = true
			return nil
		})
		defer nds.SetDatastoreMutateHook(nil)

		_, err = ndsClient.Mutate(ctx,
			nds.NewInsert(testKeys[0], testEntity{65}),
			nds.NewUpsert(testKeys[1], testEntity{65}),
			nds.NewUpdate(testKeys[2], testEntity{65}),
			nds.NewDelete(testKeys[3]),
		)

		if !deleteOk || !mutateOk || !setOk {
			t.Errorf("something went wrong: %v", err)
		}
	}
}
