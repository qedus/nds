package nds_test

import (
	"testing"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

func TestTransactionOptions(t *testing.T) {
	c, closeFunc := NewContext(t, nil)
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
