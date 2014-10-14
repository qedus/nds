package nds_test

import (
	"testing"

	"github.com/qedus/nds"

	"appengine"
	"appengine/aetest"
	"appengine/datastore"
)

func TestTransactionOptions(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type testEntity struct {
		Val int
	}

	opts := &datastore.TransactionOptions{XG: true}
	err = nds.RunInTransaction(c, func(tc appengine.Context) error {
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
	err = nds.RunInTransaction(c, func(tc appengine.Context) error {
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
