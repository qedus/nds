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
	"github.com/qedus/nds/v2"
	"github.com/qedus/nds/v2/cachers/memory"
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
	}
	testKeys := []*datastore.Key{datastore.NameKey("onErrorTest", "name", nil)}
	testObj := []testObject{{}}

	// Default implementation
	c, err := nds.NewClient(ctx, testCacher, nds.WithDatastoreClient(dsClient))
	if err != nil {
		t.Fatalf("could not make nds client due to error: %v", err)
	}

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
	onErrFn := func(ctx context.Context, err error) { gotErr = err }
	c, err = nds.NewClient(ctx, testCacher, nds.WithDatastoreClient(dsClient), nds.WithOnErrorFunc(onErrFn))
	if err != nil {
		t.Fatalf("could not make nds client due to error: %v", err)
	}

	if err = c.GetMulti(ctx, testKeys, testObj); err == nil {
		t.Errorf("Expected non-nil err, got %v", err)
	} else if !strings.Contains(gotErr.Error(), testErr.Error()) {
		t.Log(err)
		t.Errorf("Expected err `%v` to contain `%v`", testErr, gotErr)
	}

}

func TestNewClient(t *testing.T) {
	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	type args struct {
		ctx    context.Context
		cacher nds.Cacher
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"nil test",
			args{
				ctx:    context.Background(),
				cacher: nil,
			},
			false,
		},
		{
			"bad context test",
			args{
				ctx:    cctx,
				cacher: nil,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := nds.NewClient(tt.args.ctx, tt.args.cacher)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
