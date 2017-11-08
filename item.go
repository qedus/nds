package nds

import "time"

// Item is the unit of memcache gets and sets.
type item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string
	// Value is the Item's value.
	Value []byte
	// Object is the Item's value for use with a Codec.
	Object interface{}
	// Flags are server-opaque flags whose semantics are entirely up to the
	// App Engine app.
	Flags uint32
	// Expiration is the maximum duration that the item will stay
	// in the cache.
	// The zero value means the Item has no expiration time.
	// Subsecond precision is ignored.
	// This is not set when getting items.
	Expiration time.Duration
	// casID is a mcClient-opaque value used for compare-and-swap operations.
	// Zero means that compare-and-swap is not used.
	casID uint64
}