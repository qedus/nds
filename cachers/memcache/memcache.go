package memcache

import (
	"context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"

	"github.com/qedus/nds/v2"
)

type backend struct{}

// NewCacher will return a nds.Cacher backed by AppEngine's memcache.
func NewCacher() nds.Cacher {
	return &backend{}
}

func (m *backend) AddMulti(ctx context.Context, items []*nds.Item) error {
	return convertToNDSMultiError(memcache.AddMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) CompareAndSwapMulti(ctx context.Context, items []*nds.Item) error {
	return convertToNDSMultiError(memcache.CompareAndSwapMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) DeleteMulti(ctx context.Context, keys []string) error {
	return convertToNDSMultiError(memcache.DeleteMulti(ctx, keys))
}

func (m *backend) GetMulti(ctx context.Context, keys []string) (map[string]*nds.Item, error) {
	items, err := memcache.GetMulti(ctx, keys)
	if err != nil {
		return nil, convertToNDSMultiError(err)
	}
	return convertFromMemcacheItems(items), nil
}

func (m *backend) SetMulti(ctx context.Context, items []*nds.Item) error {
	return memcache.SetMulti(ctx, convertToMemcacheItems(items))
}

func convertToMemcacheItems(items []*nds.Item) []*memcache.Item {
	newItems := make([]*memcache.Item, len(items))
	for i, item := range items {
		if memcacheItem, ok := item.GetCASInfo().(*memcache.Item); ok {
			memcacheItem.Value = item.Value
			memcacheItem.Flags = item.Flags
			memcacheItem.Expiration = item.Expiration
			memcacheItem.Key = item.Key
			newItems[i] = memcacheItem
		} else {
			newItems[i] = &memcache.Item{
				Expiration: item.Expiration,
				Flags:      item.Flags,
				Key:        item.Key,
				Value:      item.Value,
			}
		}
	}
	return newItems
}

func convertFromMemcacheItems(items map[string]*memcache.Item) map[string]*nds.Item {
	newItems := make(map[string]*nds.Item)
	for key, item := range items {
		newItems[key] = &nds.Item{
			Expiration: item.Expiration,
			Flags:      item.Flags,
			Key:        item.Key,
			Value:      item.Value,
		}
		newItems[key].SetCASInfo(item)
	}
	return newItems
}

func convertToNDSMultiError(err error) error {
	if ame, ok := err.(appengine.MultiError); ok {
		me := make(nds.MultiError, len(ame))
		for i, aerr := range ame {
			switch aerr {
			case memcache.ErrNotStored:
				me[i] = nds.ErrNotStored
			case memcache.ErrCacheMiss:
				me[i] = nds.ErrCacheMiss
			case memcache.ErrCASConflict:
				me[i] = nds.ErrCASConflict
			default:
				me[i] = aerr
			}
		}
		return me
	}
	return err
}
