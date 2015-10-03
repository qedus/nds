package nds_test

import (
	"errors"
	"testing"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func TestTransactionOptions(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	opts := &datastore.TransactionOptions{XG: true}
	err := nds.RunInTransaction(c, func(tc context.Context) error {
		for i := 0; i < 4; i++ {
			key := datastore.NewIncompleteKey(tc, "Entity", nil)
			if _, err := nds.Put(tc, key, &testEntity{i}); err != nil {
				return err
			}
		}
		return nil
	}, opts)

	if err != nil {
		t.Fatal(err)
	}

	opts = &datastore.TransactionOptions{XG: false}
	err = nds.RunInTransaction(c, func(tc context.Context) error {
		for i := 0; i < 4; i++ {
			key := datastore.NewIncompleteKey(tc, "Entity", nil)
			if _, err := nds.Put(tc, key, &testEntity{i}); err != nil {
				return err
			}
		}
		return nil
	}, opts)

	if err == nil {
		t.Fatal("expected cross-group error")
	}

}

// TestClearNamespacedLocks tests to make sure that locks are cleared when
// RunInTransaction is using a namespace.
func TestClearNamespacedLocks(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	c, err := appengine.Namespace(c, "testnamespace")
	if err != nil {
		t.Fatal(err)
	}

	type testEntity struct {
		Val int
	}

	key := datastore.NewKey(c, "TestEntity", "", 1, nil)

	// Prime cache.
	if err := nds.Get(c, key, &testEntity{}); err == nil {
		t.Fatal("expected no such entity")
	} else if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	if err := nds.RunInTransaction(c, func(tc context.Context) error {

		if err := nds.Get(tc, key, &testEntity{}); err == nil {
			return errors.New("expected no such entity")
		} else if err != datastore.ErrNoSuchEntity {
			return err
		}

		if _, err := nds.Put(tc, key, &testEntity{3}); err != nil {
			return err
		}
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}

	entity := &testEntity{}
	if err := nds.Get(c, key, entity); err != nil {
		t.Fatal(err)
	}

	if entity.Val != 3 {
		t.Fatal("incorrect val")
	}
}
