package nds_test

import (
	"context"
	"log"
	"testing"

	"cloud.google.com/go/datastore"
	"google.golang.org/appengine/aetest"

	"github.com/qedus/nds"
	"github.com/qedus/nds/cachers/memcache"
)

func init() {
	appEnginePreHook = func() {
		if testing.Short() {
			return
		}
		c, closeFunc, err := aetest.NewContext()
		if err != nil {
			panic(err)
		}
		cachersGuard.Lock()
		defer cachersGuard.Unlock()
		cachers = append(cachers, cacherTestItem{ctx: c, cacher: memcache.NewCacher("")})
		appEnginePostHook = closeFunc
	}
}

func NewContext(t *testing.T) (context.Context, func()) {
	c, closeFunc, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	return c, closeFunc
}

func TestMemcacheNamespace(t *testing.T) {

	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	// Illegal namespace chars.
	cacher := memcache.NewCacher("£££")
	ndsClient, err := NewClient(c, cacher, t)
	if err != nil {
		t.Fatal(err)
	}

	key := datastore.IDKey("Entity", 1, nil)
	if err := ndsClient.Get(c, key, &testEntity{}); err == nil {
		t.Fatal("expected namespace error")
	}

	if _, err := ndsClient.Put(c, key, &testEntity{}); err == nil {
		t.Fatal("expected namespace error")
	}

	if err := ndsClient.Delete(c, key); err == nil {
		t.Fatal("expected namespace error")
	}

	if _, err := ndsClient.Mutate(c, nds.NewUpsert(key, &testEntity{})); err == nil {
		t.Fatal("expected namespace error")
	}

	if _, err := ndsClient.RunInTransaction(c, func(tx *nds.Transaction) error {
		log.Println("returnin nil")
		return nil
	}); err == nil {
		t.Fatal("expected namespace error")
	}
}
