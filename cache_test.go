package nds_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/qedus/nds/v2"
)

func TestCachersImplementations(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), CacherImplementationTest(item.ctx, item.cacher))
	}
}

func CacherImplementationTest(ctx context.Context, cacher nds.Cacher) func(t *testing.T) {
	return func(t *testing.T) {
		// Test table used to test both Add/Get cacher calls
		type args struct {
			c     context.Context
			items []*nds.Item
		}
		badCtx, cancel := context.WithCancel(ctx)
		cancel()
		var tests = []struct {
			name    string
			args    args
			addWant error
			getWant error
			setWant bool
			delWant bool
		}{
			{
				"simple",
				args{
					ctx,
					[]*nds.Item{&nds.Item{Key: "simple", Value: []byte("simple"), Flags: 32}},
				},
				nil,
				nil,
				false,
				false,
			},
			{
				"no flags",
				args{
					ctx,
					[]*nds.Item{&nds.Item{Key: "no flags", Value: []byte("no flags")}},
				},
				nil,
				nil,
				false,
				false,
			},
			{
				"multi item",
				args{
					ctx,
					[]*nds.Item{&nds.Item{Key: "multi", Value: []byte("multi"), Flags: 32}, &nds.Item{Key: "multi 2", Value: []byte("multi 2")}},
				},
				nil,
				nil,
				false,
				false,
			},
			{
				"duplicate keys",
				args{
					ctx,
					[]*nds.Item{&nds.Item{Key: "duplicate", Value: []byte("duplicate"), Flags: 32}, &nds.Item{Key: "duplicate", Value: []byte("fail")}},
				},
				nds.MultiError{nil, nds.ErrNotStored},
				nil,
				false,
				true,
			},
			{
				"bad context",
				args{
					badCtx,
					[]*nds.Item{&nds.Item{Key: "badctx", Value: []byte("badctx")}},
				},
				badCtx.Err(),
				badCtx.Err(),
				true,
				true,
			},
			{
				"0 items",
				args{
					ctx,
					[]*nds.Item{},
				},
				nil,
				nil,
				false,
				false,
			},
			{
				"nil test",
				args{
					ctx,
					nil,
				},
				nil,
				nil,
				false,
				false,
			},
			{
				"expiring item",
				args{
					ctx,
					[]*nds.Item{&nds.Item{Key: "expire", Value: []byte("expire"), Expiration: time.Second}},
				},
				nil,
				nil,
				false,
				true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("AddMulti", func(t *testing.T) {
					if got := cacher.AddMulti(tt.args.c, tt.args.items); !reflect.DeepEqual(got, tt.addWant) {
						t.Errorf("cacher.AddMulti() = %v, want %v", got, tt.addWant)
					}
				})

				t.Run("GetMulti", func(t *testing.T) {
					var keys []string = nil
					me, ok := tt.addWant.(nds.MultiError)
					if tt.args.items != nil {
						keys = make([]string, len(tt.args.items))

						for i, item := range tt.args.items {
							keys[i] = item.Key
							if item.Expiration > 0 {
								time.Sleep(item.Expiration + (10 * time.Millisecond))
							}
						}
					}
					foundSet := make(map[string]interface{})
					result, err := cacher.GetMulti(tt.args.c, keys)
					if err != tt.getWant {
						t.Errorf("wanted err %v, got %v", tt.getWant, err)
						return
					}
					if err == nil {
						if result == nil || keys == nil {
							return
						}
						for i, item := range tt.args.items {
							cacheItem, iok := result[keys[i]]
							if tt.addWant == nil || (ok && me[i] == nil) {
								if !iok && item.Expiration == 0 {
									t.Errorf("expected to find key '%s' but did not", keys[i])
								} else if iok && !itemsEqual(item, cacheItem) {
									t.Errorf("expected %v, got %v", item, cacheItem)
								}
								if item.Expiration != 0 && iok {
									t.Errorf("found key '%s' but shouldn't have", keys[i])
								}
								if iok {
									foundSet[keys[i]] = nil
								}
							} else if _, fok := foundSet[keys[i]]; iok && !fok {
								t.Errorf("found key '%s' but shouldn't have", keys[i])
							}
						}
					}
				})

				t.Run("SetMulti", func(t *testing.T) {
					if got := cacher.SetMulti(tt.args.c, tt.args.items); (got != nil) != tt.setWant {
						t.Errorf("cacher.SetMulti() = %v, want err = %v", got, tt.setWant)
					}
					if tt.setWant {
						return
					}
					keys := make([]string, len(tt.args.items))
					for i, item := range tt.args.items {
						keys[i] = item.Key
						if item.Expiration > 0 {
							time.Sleep(item.Expiration + (10 * time.Millisecond))
						}
					}
					foundSet := make(map[string]*nds.Item)
					result, err := cacher.GetMulti(tt.args.c, keys)
					if err != nil {
						t.Errorf("wanted err = nil, got %v", err)
						return
					}
					if result == nil {
						return
					}
					for _, item := range tt.args.items {
						foundSet[item.Key] = item
					}
					for key, item := range foundSet {
						cacheItem, iok := result[key]
						if !tt.setWant {
							if !iok && item.Expiration == 0 {
								t.Errorf("expected to find key '%s' but did not", key)
							} else if iok && !itemsEqual(item, cacheItem) {
								t.Errorf("expected %v, got %v", item, cacheItem)
							}
							if item.Expiration != 0 && iok {
								t.Errorf("found key '%s' but shouldn't have", key)
							}
						} else if iok {
							t.Errorf("found key '%s' but shouldn't have", key)
						}
					}
				})

				t.Run("DeleteMulti", func(t *testing.T) {
					var keys []string = nil
					if tt.args.items != nil {
						keys = make([]string, len(tt.args.items))
						for i, item := range tt.args.items {
							keys[i] = item.Key
						}
					}

					if got := cacher.DeleteMulti(tt.args.c, keys); (got != nil) != tt.delWant {
						t.Errorf("cacher.DeleteMulti() = %v, want err = %v", got, tt.delWant)
					} else if got == nil {
						if result, err := cacher.GetMulti(tt.args.c, keys); err != nil {
							t.Errorf("expected err = nil, got %v", err)
						} else if len(result) != 0 {
							t.Errorf("expected len(cacher.GetMulti()) = 0, got %d", len(result))
						}
					}
				})
			})
		}

		t.Run("CompareAndSwapMulti", func(t *testing.T) {
			succeedItem := &nds.Item{
				Key:   "Succeed",
				Value: []byte("Success"),
			}
			evictItem := &nds.Item{
				Key:   "Evict",
				Value: []byte("Evict"),
			}
			modifyValueItem := &nds.Item{
				Key:   "Modify Value",
				Value: []byte("Modify Value"),
			}
			modifyFlagsItem := &nds.Item{
				Key:   "Modify Flags",
				Value: []byte("Modify Flags"),
				Flags: 1,
			}

			testItems := []*nds.Item{
				succeedItem,
				evictItem,
				modifyValueItem,
				modifyFlagsItem,
			}

			keys := make([]string, len(testItems))
			for i, item := range testItems {
				keys[i] = item.Key
			}

			// Bad Context Test
			if err := cacher.CompareAndSwapMulti(badCtx, testItems); err != badCtx.Err() {
				t.Errorf("expected err = %v, got %v", badCtx.Err(), err)
			}

			// Set the items in cache
			if err := cacher.SetMulti(ctx, testItems); err != nil {
				t.Fatalf("expected nil err, got %v", err)
			}

			// Get the items to begin CAS operation
			result, rerr := cacher.GetMulti(ctx, keys)
			if rerr != nil {
				t.Fatalf("expected nil err, got %v", rerr)
			}
			// Validate we got all we were expecting to
			if len(result) != len(testItems) {
				t.Fatalf("expected %d items returned, got %d", len(testItems), len(result))
			}

			// Delete a CAS item
			if err := cacher.DeleteMulti(ctx, []string{evictItem.Key}); err != nil {
				t.Fatalf("expected nil err, got %v", err)
			}

			// Modify the value of one and flags of another CAS item
			modifyValueItem.Value = []byte("modified")
			modifyFlagsItem.Flags += 1
			if err := cacher.SetMulti(ctx, []*nds.Item{modifyValueItem, modifyFlagsItem}); err != nil {
				t.Fatalf("expected nil err, got %v", err)
			}

			// Update all items from Get call
			newValue := []byte("new")
			casItems := make([]*nds.Item, len(testItems))

			for i, key := range keys {
				result[key].Value = newValue
				casItems[i] = result[key]
			}

			// Do the CAS operation
			cerr := cacher.CompareAndSwapMulti(ctx, casItems)
			want := nds.MultiError{
				nil,
				nds.ErrNotStored,
				nds.ErrCASConflict,
				nds.ErrCASConflict,
			}
			if !reflect.DeepEqual(cerr, want) {
				t.Errorf("expected err %v, got %v", want, cerr)
			}

			// Validate items were updated or left alone as appropiate
			result, rerr = cacher.GetMulti(ctx, keys)
			if rerr != nil {
				t.Fatalf("expected nil err, got %v", rerr)
			}

			succeedItem.Value = newValue
			for _, item := range []*nds.Item{succeedItem, modifyFlagsItem, modifyValueItem} {
				if ritem, ok := result[item.Key]; !ok {
					t.Fatalf("was expecting to find '%s' but didn't", item.Key)
				} else if !itemsEqual(ritem, item) {
					t.Errorf("expecting %v, got %v", item, ritem)
				}
			}

			if _, ok := result[evictItem.Key]; ok {
				t.Errorf("found '%s' but shouldn't have", evictItem.Key)
			}

			// Single item, no update, add an expiration
			result[succeedItem.Key].Expiration = time.Second
			if err := cacher.CompareAndSwapMulti(ctx, []*nds.Item{result[succeedItem.Key]}); err != nil {
				t.Errorf("expecting err = nil, got %v", err)
			}

			time.Sleep(result[succeedItem.Key].Expiration + (10 * time.Millisecond))

			if result, err := cacher.GetMulti(ctx, []string{succeedItem.Key}); err != nil {
				t.Errorf("expected err = nil,  got %v", err)
			} else if len(result) != 0 {
				t.Errorf("expected no results, got %d", len(result))
			}

			// nil test
			if err := cacher.CompareAndSwapMulti(ctx, nil); err != nil {
				t.Errorf("expecting err = nil, got %v", err)
			}
		})
	}
}

func itemsEqual(a *nds.Item, b *nds.Item) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	return a.Key == b.Key && bytes.Equal(a.Value, b.Value) && a.Flags == b.Flags
}

func TestMultiError_Error(t *testing.T) {
	tests := []struct {
		name string
		m    nds.MultiError
		want string
	}{
		{
			"nil case",
			nil,
			"(0 errors)",
		},
		{
			"multi nil case",
			nds.MultiError{nil},
			"(0 errors)",
		},
		{
			"multi nil case 2",
			nds.MultiError{nil, nil},
			"(0 errors)",
		},
		{
			"single case",
			nds.MultiError{fmt.Errorf("single")},
			"single",
		},
		{
			"single nil-mixed case",
			nds.MultiError{nil, fmt.Errorf("single"), nil, nil},
			"single",
		},
		{
			"two case",
			nds.MultiError{fmt.Errorf("first"), fmt.Errorf("second")},
			"first (and 1 other error)",
		},
		{
			"two nil-mixed case",
			nds.MultiError{fmt.Errorf("first"), nil, nil, fmt.Errorf("second")},
			"first (and 1 other error)",
		},
		{
			"multi case",
			nds.MultiError{fmt.Errorf("first"), fmt.Errorf("second"), fmt.Errorf("third")},
			"first (and 2 other errors)",
		},
		{
			"multi nil-mixed case",
			nds.MultiError{nil, fmt.Errorf("first"), fmt.Errorf("second"), fmt.Errorf("third"), nil, nil},
			"first (and 2 other errors)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Error(); got != tt.want {
				t.Errorf("MultiError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestItem_SetGetCASInfo(t *testing.T) {
	testItem := &nds.Item{Key: "test"}
	testValue := "test"
	// Set the value
	testItem.SetCASInfo(testValue)

	// Validate the returned value is the correct one
	if got, ok := testItem.GetCASInfo().(string); !ok || got != testValue {
		t.Errorf("wanted %v, got %v", testValue, testItem.GetCASInfo())
	}

	// Multiple calls to SetCASInfo shouldn't alter the value
	testItem.SetCASInfo("Something")
	testItem.SetCASInfo("Different")

	// Validate the returned value is unchanged
	if got, ok := testItem.GetCASInfo().(string); !ok || got != testValue {
		t.Errorf("wanted %v, got %v", testValue, testItem.GetCASInfo())
	}
}
