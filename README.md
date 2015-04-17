# nds

[![Build Status](https://travis-ci.org/qedus/nds.svg?branch=master)](https://travis-ci.org/qedus/nds) [![Coverage Status](https://coveralls.io/repos/qedus/nds/badge.png?branch=master)](https://coveralls.io/r/qedus/nds?branch=master) [![GoDoc](https://godoc.org/github.com/qedus/nds?status.png)](https://godoc.org/github.com/qedus/nds)

Package `github.com/qedus/nds` is a datastore API for the Google App Engine (GAE) [Go Runtime Environment](https://developers.google.com/appengine/docs/go/) that uses memcache to cache all datastore requests. It is compatible with both Classic and Managed VM products. This package guarantees strong cache consistency when using `nds.Get*` and `nds.Put*`, meaning you will never get data from a stale cache.

Exposed parts of this API are the same as the official one distributed by Google ([`google.golang.org/appengine/datastore`](https://godoc.org/google.golang.org/appengine/datastore)). However, underneath `github.com/qedus/nds` uses a caching stategy similar to the GAE [Python NDB API](https://developers.google.com/appengine/docs/python/ndb/). In fact the caching strategy used here even fixes one or two of the Python NDB [caching consistency bugs](http://goo.gl/3ByVlA).

You can find the API documentation at [http://godoc.org/github.com/qedus/nds](http://godoc.org/github.com/qedus/nds).

One other benefit is that the standard `datastore.GetMulti` function only allows you to retrieve a maximum of 1000 entities at a time. The [`GetMulti`](http://godoc.org/github.com/qedus/nds#GetMulti) in this package allows you to get as many as you need (within timeout limits) by concurrently calling the datastore until your entity request is fulfilled.

## How To Use

You can use this package in *exactly* the same way you would use [`google.golang.org/appengine/datastore`](https://godoc.org/google.golang.org/appengine/datastore). However, it is important that you use `nds.Get*`, `nds.Put*`, `nds.Delete*` and `nds.RunInTransaction` entirely within your code. Do not mix use of those functions with the `google.golang.org/appengine/datastore` equivalents as you will be liable to get stale datastore entities from `github.com/qedus/nds`.

Ultimately all you need to do is find/replace the following in your codebase:

- `datastore.Get` -> `nds.Get`
- `datastore.Put` -> `nds.Put`
- `datastore.Delete` -> `nds.Delete`
- `datastore.RunInTransaction` -> `nds.RunInTransaction`

## Classic App Engine

This package has recently been converted to use [`context.Context`](http://godoc.org/golang.org/x/net/context) instead of [`appengine.Context`](https://cloud.google.com/appengine/docs/go/reference#Context). The [classic branch](https://github.com/qedus/nds/tree/classic) has an old version that uses `appengine.Context`.
