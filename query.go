package nds

import (
	"appengine"
	"appengine/datastore"
)

type Query struct {
	*datastore.Query
}

func NewQuery(kind string) *Query {
	return &Query{datastore.NewQuery(kind)}
}

func (q *Query) Ancestor(ancestor *Key) *Query {
	return &Query{q.Query.Ancestor(ancestor.Key)}
}

func (q *Query) Filter(filter string, val interface{}) *Query {
	if key, ok := val.(*Key); ok {
		val = key.Key
	}
	return &Query{q.Query.Filter(filter, val)}
}

func (q *Query) Order(field string) *Query {
	return &Query{q.Query.Order(field)}
}

func (q *Query) Project(fields ...string) *Query {
	return &Query{q.Query.Project(fields...)}
}

func (q *Query) Distinct() *Query {
	return &Query{q.Query.Distinct()}
}

func (q *Query) KeysOnly() *Query {
	return &Query{q.Query.KeysOnly()}
}

func (q *Query) Limit(limit int) *Query {
	return &Query{q.Query.Limit(limit)}
}

func (q *Query) Offset(offset int) *Query {
	return &Query{q.Query.Offset(offset)}
}

func (q *Query) EventualConsistency() *Query {
	return &Query{q.Query.EventualConsistency()}
}

func (q *Query) Start(c Cursor) *Query {
	return &Query{q.Query.Start(c.Cursor)}
}

func (q *Query) End(c Cursor) *Query {
	return &Query{q.Query.End(c.Cursor)}
}

func (q *Query) GetAll(c appengine.Context, vals interface{}) ([]*Key, error) {
	keys, err := q.Query.GetAll(c, vals)
	if err != nil {
		return nil, err
	}
	return wrapKeys(keys), nil
}

type Iterator struct {
	*datastore.Iterator
}

var Done = datastore.Done

func (q *Query) Run(c appengine.Context) *Iterator {
	return &Iterator{q.Query.Run(c)}
}

func (i *Iterator) Next(val interface{}) (*Key, error) {
	key, err := i.Iterator.Next(val)
	return &Key{key}, err
}

type Cursor struct {
	datastore.Cursor
}

func (i *Iterator) Cursor() (Cursor, error) {
	cursor, err := i.Iterator.Cursor()
	return Cursor{cursor}, err
}

func DecodeCursor(s string) (Cursor, error) {
	cursor, err := datastore.DecodeCursor(s)
	return Cursor{cursor}, err
}
