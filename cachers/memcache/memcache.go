package memcache

import (
	"context"

	"google.golang.org/appengine"
	ogmem "google.golang.org/appengine/memcache"

	"github.com/qedus/nds"
)

// TODO: Convert appengine.MultiError to nds.MultiError for consistency

type memcache struct {
	// namespace is the namespace where all memcached entities are
	// stored.
	namespace string
}

// NewCacher will return a nds.Cacher backed by AppEngine's memcache,
// utilizing the provided namespace, if any.
func NewCacher(namespace string) nds.Cacher {
	return &memcache{namespace}
}

func (m *memcache) NewContext(c context.Context) (context.Context, error) {
	return appengine.Namespace(c, m.namespace)
}

func (m *memcache) AddMulti(c context.Context, items []*nds.Item) error {
	return ogmem.AddMulti(c, convertToMemcacheItems(items))
}

func (m *memcache) CompareAndSwapMulti(c context.Context, items []*nds.Item) error {
	return ogmem.CompareAndSwapMulti(c, convertToMemcacheItems(items))
}

func (m *memcache) DeleteMulti(c context.Context, keys []string) error {
	return ogmem.DeleteMulti(c, keys)
}

func (m *memcache) GetMulti(c context.Context, keys []string) (map[string]*nds.Item, error) {
	items, err := ogmem.GetMulti(c, keys)
	if err != nil {
		return nil, err
	}
	return convertFromMemcacheItems(items), nil
}

func (m *memcache) SetMulti(c context.Context, items []*nds.Item) error {
	return ogmem.SetMulti(c, convertToMemcacheItems(items))
}

func convertToMemcacheItems(items []*nds.Item) []*ogmem.Item {
	newItems := make([]*ogmem.Item, len(items))
	for i, item := range items {
		if memcacheItem, ok := item.GetCASInfo().(*ogmem.Item); ok {
			memcacheItem.Value = item.Value
			memcacheItem.Flags = item.Flags
			memcacheItem.Expiration = item.Expiration
			memcacheItem.Key = item.Key
			newItems[i] = memcacheItem
		} else {
			newItems[i] = &ogmem.Item{
				Expiration: item.Expiration,
				Flags:      item.Flags,
				Key:        item.Key,
				Value:      item.Value,
			}
		}
	}
	return newItems
}

func convertFromMemcacheItems(items map[string]*ogmem.Item) map[string]*nds.Item {
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
