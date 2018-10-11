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

type Cacher interface {
	NewContext(c context.Context) (context.Context, error)
	AddMulti(c context.Context, items []*Item) error
	CompareAndSwapMulti(c context.Context, items []*Item) error
	DeleteMulti(c context.Context, key []string) error
	GetMulti(c context.Context, key []string) (map[string]*Item, error)
	SetMulti(c context.Context, item []*Item) error
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
