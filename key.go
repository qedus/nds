package nds

import (
	"appengine"
	"appengine/datastore"
)

type Key struct {
	key *datastore.Key
}

func unwrapKeys(keys []*Key) []*datastore.Key {
	datastoreKeys := make([]*datastore.Key, len(keys))
	for i, k := range keys {
		datastoreKeys[i] = k.key
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
		parentKey = parent.key
	}
	return &Key{datastore.NewIncompleteKey(c, kind, parentKey)}
}

func NewKey(c appengine.Context,
	kind, stringID string, intID int64, parent *Key) *Key {
	var parentKey *datastore.Key
	if parent != nil {
		parentKey = parent.key
	}
	return &Key{datastore.NewKey(c, kind, stringID, intID, parentKey)}
}

func (k *Key) Parent() *Key {
	return &Key{k.key.Parent()}
}

func (k *Key) AppID() string {
	return k.key.AppID()
}

func (k *Key) Namespace() string {
	return k.key.Namespace()
}

func (k *Key) Encode() string {
	return k.key.Encode()
}

func (k *Key) GobDecode(buf []byte) error {
	key := &datastore.Key{}
	if err := key.GobDecode(buf); err != nil {
		return err
	}
	k.key = key
	return nil
}

func (k *Key) GobEncode() ([]byte, error) {
	return k.key.GobEncode()
}

func (k *Key) MarshalJSON() ([]byte, error) {
	return k.key.MarshalJSON()
}

func (k *Key) UnmarshalJSON(buf []byte) error {
	key := &datastore.Key{}
	if err := key.UnmarshalJSON(buf); err != nil {
		return err
	}
	k.key = key
	return nil
}

func (k *Key) Equal(o *Key) bool {
	return k.key.Equal(o.key)
}

func (k *Key) Incomplete() bool {
	return k.key.Incomplete()
}

func DecodeKey(encoded string) (*Key, error) {
	key, err := datastore.DecodeKey(encoded)
	if err != nil {
		return nil, err
	}
	return &Key{key}, err
}

func (k *Key) IntID() int64 {
	return k.key.IntID()
}

func (k *Key) StringID() string {
	return k.key.StringID()
}

func (k *Key) String() string {
	return k.key.String()
}

func AllocateIDs(c appengine.Context,
	kind string, parent *Key, n int) (low, high int64, err error) {

	var parentKey *datastore.Key
	if parent != nil {
		parentKey = parent.key
	}

	return datastore.AllocateIDs(c, kind, parentKey, n)
}
