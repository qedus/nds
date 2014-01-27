package nds

import (
	"appengine"
	"appengine/datastore"
)

func Get(c appengine.Context, key *datastore.Key, dst interface{}) error {
	return datastore.Get(c, key, dst)
}

func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	return datastore.Put(c, key, src)
}
