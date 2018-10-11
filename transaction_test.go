package nds_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds"
)

func TestTransactionSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestTransactionOptions", TransactionOptionsTest(item.ctx, item.cacher))
			t.Run("TestClearNamespacedLocks", ClearNamespacedLocksTest(item.ctx, item.cacher))
		})
	}
}

func TransactionOptionsTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		// All Transaction are cross group enabled up to 25 groups
		_, err = ndsClient.RunInTransaction(c, func(tx *nds.Transaction) error {
			for i := 0; i < 25; i++ {
				key := datastore.IncompleteKey("Entity", nil)
				if _, err := tx.Put(key, &testEntity{i}); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		// All Transaction are cross group enabled up to 25 groups
		_, err = ndsClient.RunInTransaction(c, func(tx *nds.Transaction) error {
			for i := 0; i < 26; i++ {
				key := datastore.IncompleteKey("Entity", nil)
				if _, err := tx.Put(key, &testEntity{i}); err != nil {
					return err
				}
			}
			return nil
		})

		if err == nil {
			t.Fatal("expected cross-group error")
		}
	}

}

// ClearNamespacedLocksTest tests to make sure that locks are cleared when
// RunInTransaction is using a namespace.
func ClearNamespacedLocksTest(c context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		ndsClient, err := NewClient(c, cacher)
		if err != nil {
			t.Fatal(err)
		}

		ns := "testnamespace"

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("TestEntity", 1, nil)
		key.Namespace = ns

		// Prime cache.
		if err := ndsClient.Get(c, key, &testEntity{}); err == nil {
			t.Fatal("expected no such entity")
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}

		if _, err := ndsClient.RunInTransaction(c, func(tx *nds.Transaction) error {

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
		if err := ndsClient.Get(c, key, entity); err != nil {
			t.Fatal(err)
		}

		if entity.Val != 3 {
			t.Fatal("incorrect val")
		}

		// Cleanup
		ndsClient.Delete(c, key)
	}
}
