package nds_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds/v2"
)

func TestTransactionSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestTransactionOptions", TransactionOptionsTest(item.ctx, item.cacher))
			t.Run("TestClearNamespacedLocks", ClearNamespacedLocksTest(item.ctx, item.cacher))
			t.Run("TestTransactionGet", TransactionGetTest(item.ctx, item.cacher))
			t.Run("TestRunInTransactionError", RunInTransactionErrorTest(item.ctx, item.cacher))
			t.Run("TestTransactionTracking", TransactionTrackingTest(item.ctx, item.cacher))
			t.Run("TestTransactionNewError", TransactionNewErrorTest(item.ctx, item.cacher))
			t.Run("TestTransactionCommit", TransactionCommitTest(item.ctx, item.cacher))
			t.Run("TestTransactionCommitError", TransactionCommitErrorTest(item.ctx, item.cacher))
			t.Run("TestTransactionRollback", TransactionRollbackTest(item.ctx, item.cacher))
			t.Run("TestTransactionQueryHelper", TransactionQueryHelperTest(item.ctx, item.cacher))

		})
	}
}

// Get calls should not use the cache
func TransactionGetTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		wasCalled := false
		testCacher := &mockCacher{
			cacher: cacher,
			getMultiHook: func(_ context.Context, _ []string) (map[string]*nds.Item, error) {
				wasCalled = true
				return nil, errors.New("should not be called")
			},
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Put something in the datastore to fetch
		type testEntity struct {
			Value int64
		}
		entity := &testEntity{56}

		key := datastore.IncompleteKey("TransactionGetTest", nil)
		if key, err = ndsClient.Put(ctx, key, entity); err != nil {
			t.Fatalf("could not store entity due to error: %v", err)
		}

		t.Run("Transaction_Get", func(t *testing.T) {
			ndsClient, err := NewClient(ctx, testCacher, t, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
				var txTest testEntity
				if err := tx.Get(key, &txTest); err != nil {
					return err
				}
				if txTest.Value != entity.Value {
					return fmt.Errorf("expected Value=%d, got %d", entity.Value, txTest.Value)
				}
				return nil
			})
			if err != nil {
				t.Errorf("expected err=nil, got %v", err)
			}
			if wasCalled {
				t.Errorf("cache was called and shouldn't have been")
				wasCalled = false
			}
		})

		t.Run("Transaction_GetMulti", func(t *testing.T) {
			ndsClient, err := NewClient(ctx, testCacher, t, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
				txTest := make([]testEntity, 1)
				if err := tx.GetMulti([]*datastore.Key{key}, txTest); err != nil {
					return err
				}
				if len(txTest) != 1 || txTest[0].Value != entity.Value {
					return fmt.Errorf("expected Value=%d, got %v", entity.Value, txTest)
				}
				return nil
			})
			if err != nil {
				t.Errorf("expected err=nil, got %v", err)
			}
			if wasCalled {
				t.Errorf("cache was called and shouldn't have been")
			}
		})
	}
}

// If an error occured in the transaction, do NOT try and lock keys
func RunInTransactionErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		wasCalled := false
		testCacher := &mockCacher{
			cacher: cacher,
			setMultiHook: func(_ context.Context, _ []*nds.Item) error {
				wasCalled = true
				return errors.New("should not be called")
			},
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}
		type testEntity struct {
			Value int64
		}

		var expectedErr = errors.New("expected")

		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			if _, err := tx.Put(datastore.IncompleteKey("RunInTransactionErrorTest", nil), &testEntity{54}); err != nil {
				return err
			}
			return expectedErr
		})
		if err != expectedErr {
			t.Errorf("expected err = expected, got %v", err)
		}
		if wasCalled {
			t.Errorf("cache was called and shouldn't have been")
		}
	}
}

func TransactionTrackingTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		testKeys := []*datastore.Key{
			datastore.NameKey("TransactionTrackingTest", "delete", nil),
			datastore.NameKey("TransactionTrackingTest", "deleteMulti", nil),
			datastore.NameKey("TransactionTrackingTest", "put", nil),
			datastore.NameKey("TransactionTrackingTest", "putMulti", nil),
			datastore.NameKey("TransactionTrackingTest", "insertMutation", nil),
			datastore.NameKey("TransactionTrackingTest", "upsertMutation", nil),
			datastore.NameKey("TransactionTrackingTest", "updateMutation", nil),
			datastore.NameKey("TransactionTrackingTest", "deleteMutation", nil),
		}

		type testEntity struct {
			Value int
		}

		expectedKeys := make([]string, len(testKeys))
		for i, key := range testKeys {
			expectedKeys[i] = nds.CreateCacheKey(key)
			if key.Name == "insertMutation" {
				continue
			}
			if _, err = ndsClient.Put(ctx, key, &testEntity{i}); err != nil {
				t.Fatalf("could not put entity: %v", err)
			}
		}

		cacheOk := false
		testCacher.setMultiHook = func(ctx context.Context, items []*nds.Item) error {
			if got := len(items); got != len(expectedKeys) {
				return fmt.Errorf("expected len(keys) = %d, wanted %d", got, len(expectedKeys))
			}
			for i := range items {
				if items[i].Key != expectedKeys[i] {
					return fmt.Errorf("expected key = %s, got %s in position %d", expectedKeys[i], items[i].Key, i)
				}
			}
			cacheOk = true
			return cacher.SetMulti(ctx, items)
		}
		newEntity := testEntity{100}
		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			if _, err := tx.Put(testKeys[0], &newEntity); err != nil {
				return err
			}
			if _, err := tx.PutMulti([]*datastore.Key{testKeys[1]}, []testEntity{newEntity}); err != nil {
				return err
			}
			if err := tx.Delete(testKeys[2]); err != nil {
				return err
			}
			if err := tx.DeleteMulti([]*datastore.Key{testKeys[3]}); err != nil {
				return err
			}
			mutations := []*nds.Mutation{
				nds.NewInsert(testKeys[4], &newEntity),
				nds.NewUpsert(testKeys[5], &newEntity),
				nds.NewUpdate(testKeys[6], &newEntity),
				nds.NewDelete(testKeys[7]),
			}
			if _, err := tx.Mutate(mutations...); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Errorf("expected err=nil, got %v", err)
		}
		if !cacheOk {
			t.Errorf("cache not processed correctly")
		}

		// Cleanup
		_ = ndsClient.DeleteMulti(ctx, testKeys)
	}
}

func TransactionNewErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		badctx, cancel := context.WithCancel(ctx)
		cancel()

		_, terr := ndsClient.NewTransaction(badctx)
		if terr == nil {
			t.Error("expected error, got nil")
		}
	}
}

func TransactionCommitTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int
		}

		key := datastore.IncompleteKey("TransactionCommitTest", nil)
		entity := &testEntity{20}
		if key, err = ndsClient.Put(ctx, key, entity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		txn, terr := ndsClient.NewTransaction(ctx)
		if terr != nil {
			t.Fatalf("could not start transaciton: %v", err)
		}

		newEntity := &testEntity{100}
		if _, err = txn.Put(key, newEntity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		if _, err = txn.Commit(); err != nil {
			t.Fatalf("could not commit transaction: %v", err)
		}

		var dest testEntity
		if err = ndsClient.Get(ctx, key, &dest); err != nil {
			t.Fatalf("could not get entity: %v", err)
		}

		if dest.Value != newEntity.Value {
			t.Errorf("expected dest.Value = %d, got %d", newEntity.Value, dest.Value)
		}
	}
}

func TransactionCommitErrorTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		testCacher := &mockCacher{
			cacher: cacher,
		}
		ndsClient, err := NewClient(ctx, testCacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int
		}

		key := datastore.IncompleteKey("TransactionCommitErrorTest", nil)
		entity := &testEntity{20}
		if key, err = ndsClient.Put(ctx, key, entity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		txn, terr := ndsClient.NewTransaction(ctx)
		if terr != nil {
			t.Fatalf("could not start transaciton: %v", err)
		}

		newEntity := &testEntity{100}
		if _, err = txn.Put(key, newEntity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		testCacher.setMultiHook = func(_ context.Context, _ []*nds.Item) error {
			return errors.New("failure")
		}

		if _, err = txn.Commit(); err == nil {
			t.Fatal("expected err, got nil")
		}

		var dest testEntity
		if err = ndsClient.Get(ctx, key, &dest); err != nil {
			t.Fatalf("could not get entity: %v", err)
		}

		if dest.Value != entity.Value {
			t.Errorf("expected dest.Value = %d, got %d", entity.Value, dest.Value)
		}
	}
}

func TransactionRollbackTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int
		}

		key := datastore.IncompleteKey("TransactionRollbackTest", nil)
		entity := &testEntity{20}
		if key, err = ndsClient.Put(ctx, key, entity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		txn, terr := ndsClient.NewTransaction(ctx)
		if terr != nil {
			t.Fatalf("could not start transaciton: %v", err)
		}

		newEntity := &testEntity{100}
		if _, err = txn.Put(key, newEntity); err != nil {
			t.Fatalf("error saving entity: %v", err)
		}

		if err = txn.Rollback(); err != nil {
			t.Fatalf("could not rollback transaction: %v", err)
		}

		var dest testEntity
		if err = ndsClient.Get(ctx, key, &dest); err != nil {
			t.Fatalf("could not get entity: %v", err)
		}

		if dest.Value != entity.Value {
			t.Errorf("expected dest.Value = %d, got %d", entity.Value, dest.Value)
		}
	}
}

func TransactionQueryHelperTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	// TODO: With eventual migration to Firestore Datastore Mode - this test becomes obsolete
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		dsclient, err := datastore.NewClient(ctx, "")
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Value int64
		}
		entity := &testEntity{96}
		ancestor := datastore.NameKey("TransactionQueryHelperTest", "ancestor", nil)

		key := datastore.IncompleteKey("TransactionQueryHelperTest", ancestor)
		if key, err = ndsClient.Put(ctx, key, entity); err != nil {
			t.Fatalf("could not store entity due to error: %v", err)
		}

		defer func() { _ = ndsClient.Delete(ctx, key) }() // Cleanup

		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			var dest []testEntity
			q := datastore.NewQuery("TransactionQueryHelperTest").Ancestor(ancestor)
			q = tx.Query(q)

			if _, err := dsclient.GetAll(ctx, q, &dest); err != nil {
				return err
			}
			if len(dest) != 1 || dest[0].Value != entity.Value {
				return fmt.Errorf("expected Value=%d, got %d", entity.Value, dest[0].Value)
			}
			return nil
		})
		if err != nil {
			t.Errorf("expected err=nil, got %v", err)
		}

		// only ancestor queries are allowed in transactions!
		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			var dest []testEntity
			q := datastore.NewQuery("TransactionQueryHelperTest")
			q = tx.Query(q)

			if _, err := dsclient.GetAll(ctx, q, &dest); err != nil {
				return err
			}
			return nil
		})
		if err == nil {
			t.Error("expected err, got nil")
		}
	}
}

func TransactionOptionsTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		// Should be able to write to datastore, no issues
		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			for i := 0; i < 25; i++ {
				key := datastore.IncompleteKey("TransactionOptionsTest", nil)
				if _, err := tx.Put(key, &testEntity{i}); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		// ReadOnly transaction should result in an error
		_, err = ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {
			for i := 0; i < 25; i++ {
				key := datastore.IncompleteKey("TransactionOptionsTest", nil)
				if _, err := tx.Put(key, &testEntity{i}); err != nil {
					return err
				}
			}
			return nil
		}, datastore.ReadOnly)

		if err == nil {
			t.Fatal("expected readonly error")
		}
	}

}

// ClearNamespacedLocksTest tests to make sure that locks are cleared when
// RunInTransaction is using a namespace.
func ClearNamespacedLocksTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		ns := "testnamespace"

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("ClearNamespacedLocksTest", 1, nil)
		key.Namespace = ns

		// Prime cache.
		if err := ndsClient.Get(ctx, key, &testEntity{}); err == nil {
			t.Fatal("expected no such entity")
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}

		if _, err := ndsClient.RunInTransaction(ctx, func(tx *nds.Transaction) error {

			if err := tx.Get(key, &testEntity{}); err == nil {
				return errors.New("expected no such entity")
			} else if err != datastore.ErrNoSuchEntity {
				return err
			}

			if _, err := tx.Put(key, &testEntity{3}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		entity := &testEntity{}
		if err := ndsClient.Get(ctx, key, entity); err != nil {
			t.Fatal(err)
		}

		if entity.Val != 3 {
			t.Fatal("incorrect val")
		}

		// Cleanup
		_ = ndsClient.Delete(ctx, key)
	}
}
