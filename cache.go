package nds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MultiError is returned by batch operations when there are errors with
// particular elements. Errors will be in a one-to-one correspondence with
// the input elements; successful elements will have a nil entry.
type MultiError []error

func (m MultiError) Error() string {
	s, n := "", 0
	for _, e := range m {
		if e != nil {
			if n == 0 {
				s = e.Error()
			}
			n++
		}
	}
	switch n {
	case 0:
		return "(0 errors)"
	case 1:
		return s
	case 2:
		return s + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", s, n-1)
}

var (
	// ErrCacheMiss means an item was not found in cache
	ErrCacheMiss = errors.New("nds: cache miss")
	// ErrCASConflict means the cache item was modified between Get and CAS calls
	ErrCASConflict = errors.New("nds: cas conflict")
	// ErrNotStored means that an item was not stored due to a condition check failure (e.g. during an Add or CompareAndSwap call)
	ErrNotStored = errors.New("nds: not stored")
)

// Cacher represents a cache backend that can be used by nds.
type Cacher interface {
	// NewContext returns the same or a derivative of the passed in context. The returned context will
	// be used in subsequent calls to the other Cacher functions.
	NewContext(c context.Context) (context.Context, error)
	// AddMulti adds each provided Item into the cache if and only if the key for the item is not
	// currently in use. For any item that was not stored due to a key conflict, a MultiError is returned
	// with ErrNotStored in the corresponding index for that item. There may be other errors for each MultiError index.
	AddMulti(c context.Context, items []*Item) error
	// CompareAndSwapMulti will set each item in the cache if the item was unchanged since it was last got.
	// The items are modified versions of items returned by the GetMulti call for this Cacher.
	// If any item was not able to be stored, a MultiError will be returned with ErrCASConflict if the value in
	// cache was changed since the GetMulti call, or ErrNotStored if the key was evicted or could otherwise not be
	// stored in the corresponding index for that item. There may be other errors for each MultiError index.
	CompareAndSwapMulti(c context.Context, items []*Item) error
	// DeleteMulti deletes all keys provided in the cache. It is not necessary to track individual key misses though
	// the proper way would be to return a MultiError with ErrCacheMiss stored in the corresponding index for that key.
	DeleteMulti(c context.Context, keys []string) error
	// GetMulti fetches all provided keys from the cache, returning a map[string]*Item filled with all Items
	// it was able to retrieve, using the item key as the lookup key. The cache implementation should utilize the SetCAS/
	// GetCAS info calls of each Item as required so subsequent calls to CompareAndSwapMulti can correctly execute. No
	// error should be returned for any key misses.
	GetMulti(c context.Context, keys []string) (map[string]*Item, error)
	// SetMulti sets all provided items in the cache, regardless of whether or not the key is already in use and the
	// value of that stored item. If any item could not be stored A MultiError should be returned with an error in the
	// corresponding index for that item. Note: ErrNotStored is not a valid error to be used for any error returned.
	SetMulti(c context.Context, items []*Item) error
}

// Item is the unit of Cacher gets and sets.
// Tkaen from google.golang.org/appengine/memcache
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string
	// Value is the Item's value.
	Value []byte
	// Flags are server-opaque flags whose semantics are entirely up to the
	// app.
	Flags uint32
	// Expiration is the maximum duration that the item will stay
	// in the cache.
	// The zero value means the Item has no expiration time.
	// Subsecond precision is ignored.
	// This is not set when getting items.
	Expiration time.Duration
	// casInfo is a client-opaque value used for compare-and-swap operations.
	// nil means that a compare-and-swap is not used.
	casInfo interface{}
	casOnce sync.Once
}

// SetCASInfo is used by the Cacher implementation as needed for use with
// compare-and-swap operations. It will only set the value for the item once
// with subsequent calls being silently discarded.
func (i *Item) SetCASInfo(value interface{}) {
	i.casOnce.Do(func() {
		i.casInfo = value
	})
}

// GetCASInfo will return the value stored for this item for use with compare-and-swap
// operations.
func (i *Item) GetCASInfo() interface{} {
	return i.casInfo
}
