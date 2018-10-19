package nds_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/qedus/nds"
	"github.com/qedus/nds/cachers/memory"
)

func TestClient_onError(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("onError Test")

	testCacher := &mockCacher{
		cacher: memory.NewCacher(),
		getMultiHook: func(_ context.Context, _ []string) (map[string]*nds.Item, error) {
			return nil, testErr
		},
	}

	dsClient, err := datastore.NewClient(ctx, "")
	if err != nil {
		t.Fatalf("could not get datastore client: %v", err)
	}
	type testObject struct {
		name string
	}
	testKeys := []*datastore.Key{datastore.NameKey("onErrorTest", "name", nil)}
	testObj := []testObject{{}}

	// Default implementation
	c := nds.NewClient(ctx, &nds.Config{
		CacheBackend:    testCacher,
		OnError:         nil,
		DatastoreClient: dsClient,
	})

	buf := bytes.NewBuffer(nil)
	log.SetOutput(buf)
	defer log.SetOutput(os.Stderr)
	if err := c.GetMulti(ctx, testKeys, testObj); err == nil {
		t.Errorf("Expected non-nil err, got %v", err)
	} else if !strings.Contains(buf.String(), testErr.Error()) {
		t.Log(err)
		t.Errorf("Expected log message to have `%s`, got `%s`", testErr.Error(), buf.String())
	}

	// Custom implementation
	var gotErr error
	c = nds.NewClient(ctx, &nds.Config{
		CacheBackend:    testCacher,
		OnError:         func(ctx context.Context, err error) { gotErr = err },
		DatastoreClient: dsClient,
	})

	if err := c.GetMulti(ctx, testKeys, testObj); err == nil {
		t.Errorf("Expected non-nil err, got %v", err)
	} else if !strings.Contains(gotErr.Error(), testErr.Error()) {
		t.Log(err)
		t.Errorf("Expected err `%v` to contain `%v`", testErr, gotErr)
	}

}
