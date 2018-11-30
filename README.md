# nds (v2 - EXPERIMENTAL)

[![Build Status](https://travis-ci.org/qedus/nds.svg?branch=master)](https://travis-ci.org/qedus/nds) [![Coverage Status](https://coveralls.io/repos/github/qedus/nds/badge.svg?branch=master)](https://coveralls.io/github/qedus/nds?branch=master) [![GoDoc](https://godoc.org/github.com/qedus/nds?status.png)](https://godoc.org/github.com/qedus/nds)

Package `github.com/qedus/nds` is a Google Cloud Datastore API for Go that uses a cache backend to cache all datastore requests. Memcache is only supported on Google AppEngine Standard, but the package can be used for any other implemented cache backend on any platform (local, Google Compute, AWS, etc.). This package guarantees strong cache consistency when using `nds.Client.Get*` and `nds.Client.Put*`, meaning you will never get data from a stale cache.

Exposed parts of this API are the same as the official one distributed by Google ([`code.google.com/go/datastore`](https://godoc.org/code.google.com/go/datastore)). However, underneath `github.com/qedus/nds` uses a caching stategy similar to the GAE [Python NDB API](https://developers.google.com/appengine/docs/python/ndb/). In fact the caching strategy used here even fixes one or two of the Python NDB [caching consistency bugs](http://goo.gl/3ByVlA).

You can find the API documentation at [http://godoc.org/github.com/qedus/nds](http://godoc.org/github.com/qedus/nds).

One other benefit is that the standard `datastore.Client.GetMulti`, `datastore.Client.PutMulti` and `datastore.Client.DeleteMulti` functions only allow you to work with a maximum of 1000, 500 and 500 entities per call respectively. The `nds.Client.GetMulti`, `nds.Client.PutMulti` and `nds.Client.DeleteMulti` functions in this package allow you to work with as many entities as you need (within timeout limits) by concurrently calling the appropriate datastore function until your request is fulfilled.

## How To Use

You can use this package in _exactly_ the same way you would use [`code.google.com/go/datastore.Client`](https://godoc.org/cloud.google.com/go/datastore#Client) for methods provided by `nds.Client`. However,it is important that you use a `nds.Client` entirely within your code. Do not mix use of those functions with the `code.google.com/go/datastore.Client` equivalents as you will be liable to get stale datastore entities from `github.com/qedus/nds`.

Ultimately all you need to do is:

- import github.com/qedus/nds/v2
- use `nds.NewClient` instead of `datastore.NewClient`, providing a cache configuration to the new client creation function.
- replace `datastore.Transaction` -> `nds.Transaction`
- if using `(*datastore.Query).Transaction` for queries within transactions, switch to the `(*nds.Transaction).Query` helper.
