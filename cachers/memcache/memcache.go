package memcache

import (
	"context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"

	"github.com/qedus/nds"
)

type backend struct {
	// namespace is the namespace where all memcached entities are
	// stored.
	namespace string
}

// NewCacher will return a nds.Cacher backed by AppEngine's memcache,
// utilizing the provided namespace, if any.
func NewCacher(namespace string) nds.Cacher {
	return &backend{namespace}
}

func (m *backend) NewContext(c context.Context) (context.Context, error) {
	return appengine.Namespace(c, m.namespace)
}

func (m *backend) AddMulti(c context.Context, items []*nds.Item) error {
	return convertToNDSMultiError(memcache.AddMulti(c, convertToMemcacheItems(items)))
}

func (m *backend) CompareAndSwapMulti(c context.Context, items []*nds.Item) error {
	return convertToNDSMultiError(memcache.CompareAndSwapMulti(c, convertToMemcacheItems(items)))
}

func (m *backend) DeleteMulti(c context.Context, keys []string) error {
	return convertToNDSMultiError(memcache.DeleteMulti(c, keys))
}

func (m *backend) GetMulti(c context.Context, keys []string) (map[string]*nds.Item, error) {
	items, err := memcache.GetMulti(c, keys)
	if err != nil {
		return nil, convertToNDSMultiError(err)
	}
	return convertFromMemcacheItems(items), nil
}

func (m *backend) SetMulti(c context.Context, items []*nds.Item) error {
	return memcache.SetMulti(c, convertToMemcacheItems(items))
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
