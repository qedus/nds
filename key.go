package nds

import (
	"appengine"
	"appengine/datastore"
)

type Key struct {
	*datastore.Key
}

func unwrapKeys(keys []*Key) []*datastore.Key {
	datastoreKeys := make([]*datastore.Key, len(keys))
	for i, k := range keys {
		datastoreKeys[i] = k.Key
	}
	return datastoreKeys
}

func wrapKeys(keys []*datastore.Key) []*Key {
	ndsKeys := make([]*Key, len(keys))
	for i, k := range keys {
		ndsKeys[i] = &Key{k}
	}
	return ndsKeys
}

func NewIncompleteKey(c appengine.Context, kind string, parent *Key) *Key {
	var parentKey *datastore.Key
	if parent != nil {
		parentKey = parent.Key
	}
	return &Key{datastore.NewIncompleteKey(c, kind, parentKey)}
}

func NewKey(c appengine.Context,
	kind, stringID string, intID int64, parent *Key) *Key {
	var parentKey *datastore.Key
	if parent != nil {
		parentKey = parent.Key
	}
	return &Key{datastore.NewKey(c, kind, stringID, intID, parentKey)}
}

func (k *Key) Parent() *Key {
	return &Key{k.Key.Parent()}
}

func (k *Key) Equal(o *Key) bool {
	return k.Key.Equal(o.Key)
}

func DecodeKey(encoded string) (*Key, error) {
	key, err := datastore.DecodeKey(encoded)
	if err != nil {
		return nil, err
	}
	return &Key{key}, err
}

func (k *Key) GobDecode(buf []byte) error {
	key := &datastore.Key{}
	if err := key.GobDecode(buf); err != nil {
		return err
	}
	k.Key = key
	return nil
}

func (k *Key) UnmarshalJSON(buf []byte) error {

	key := &datastore.Key{}
	if err := key.UnmarshalJSON(buf); err != nil {
		return err
	}
	k.Key = key
	return nil
}

func AllocateIDs(c appengine.Context,
	kind string, parent *Key, n int) (low, high int64, err error) {

	var parentKey *datastore.Key
	if parent != nil {
		parentKey = parent.Key
	}

	return datastore.AllocateIDs(c, kind, parentKey, n)
}
