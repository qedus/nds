# nds

Package `github.com/qedus/nds` is a datastore API for the Google App Engine (GAE) [Go Runtime Environment](https://developers.google.com/appengine/docs/go/).

Exposed parts of this API are identical to the official one distributed by Google ([`appengine/datastore`](https://developers.google.com/appengine/docs/go/datastore/reference)). However, underneath `github.com/qedus/nds` works just like the GAE [Python NDB API](https://developers.google.com/appengine/docs/python/ndb/). This means that you get local memory and memcache caching for free. It also has the strong cache consistency guarantees like NDB which I have not seen in other Go datastore APIs.

You can find the API documentation at [http://godoc.org/github.com/qedus/nds](http://godoc.org/github.com/qedus/nds). This API only exposes `Get`, `GetMulti`, `Put`, `PutMulti`, `Delete`, `DeleteMulti` and `RunInTransaction` functions as they are the only ones needed to fully make use of local memory and memcache caching. You can carry on using `appengine/datastore` for all other datastore operations.

One other benefit is that the standard `datastore.GetMulti` function only allows you to retrieve a maximum of 1000 entities at a time. The [`GetMulti`](http://godoc.org/github.com/qedus/nds#GetMulti) in this package allows you to get as many as you need (within timeout limits) by concurrently calling the datastore until your entity request is fulfilled.

## How To Use

You can use this package in *exactly* the same way you would use `appengine/datastore`. However if you want to make use of local memory cache and memcache then you must use a `appengine.Context` created from this package's [`NewContext`](http://godoc.org/github.com/qedus/nds#NewContext) function as so:

```go
import (
    "appengine"
    "appengine/datastore"
    "github.com/qedus/nds"
)

type MyEntity struct {
    Property int
}

func MyDatastoreFunc(c appengine.Context) error {
    // This line is important if you want to used memory and memcaching.
    // If it is not included then the nds functions will act just like
    // the standard SDK functions with no caching.
    ndsc := nds.NewContext(c)
    
    key := datastore.NewKey(ndsc, "MyEntity", "enity_key", 0, nil)
    value := &MyEntity{}
    
    // This will check local memory (attached to this ndsc context),
    // memcache and then the datastore to find key.
    if err := nds.Get(ndsc, key, value); err != nil {
        return err
    }
    
    value.Property = 23
    
    // This will update local memory, memcache and the datastore
    // with the updated entity.
    if _, err := nds.Put(ndsc, key, value); err != nil {
        return err
    }
    return nil
}

```

## Warning
It is important not to mix usage of functions between `appengine/datastore` and `github.com/qedus/nds` within your app. You will be liable to get stale datastore entities as `github.com/qedus/nds` goes to great lengths to keep caches in sync with the datastore.
