# nds

[![Build Status](https://travis-ci.org/qedus/nds.svg?branch=master)](https://travis-ci.org/qedus/nds) [![Coverage Status](https://coveralls.io/repos/qedus/nds/badge.png?branch=master)](https://coveralls.io/r/qedus/nds?branch=master) [![GoDoc](https://godoc.org/github.com/qedus/nds?status.png)](https://godoc.org/github.com/qedus/nds)

Package `github.com/qedus/nds` is a datastore API for the Google App Engine (GAE) [Go Runtime Environment](https://developers.google.com/appengine/docs/go/) that uses memcache to cache all datastore requests. This package guarantees strong cache consistency, meaning you will never get data from a stale cache.

Exposed parts of this API are the same as the official one distributed by Google ([`appengine/datastore`](https://developers.google.com/appengine/docs/go/datastore/reference)). However, underneath `github.com/qedus/nds` uses a caching stategy similar to the GAE [Python NDB API](https://developers.google.com/appengine/docs/python/ndb/). In fact the caching strategy used here even fixes one or two of the Python NDB [caching consistency bugs](http://goo.gl/3ByVlA).

You can find the API documentation at [http://godoc.org/github.com/qedus/nds](http://godoc.org/github.com/qedus/nds).

One other benefit is that the standard `datastore.GetMulti` function only allows you to retrieve a maximum of 1000 entities at a time. The [`GetMulti`](http://godoc.org/github.com/qedus/nds#GetMulti) in this package allows you to get as many as you need (within timeout limits) by concurrently calling the datastore until your entity request is fulfilled.

## How To Use

You can use this package in *exactly* the same way you would use `appengine/datastore`. However, it is important that you use `nds.Get*`, `nds.Put*` and `nds.Delete*` eitirly within your code and do not mix use of those functions between `appengine/datastore` and `github.com/qedus/nds` within your app as you will be liable to get stale datastore entities as `github.com/qedus/nds` goes to great lengths to keep caches in sync with the datastore.

Ultimately all you need to do is find/replace the following in your codebase:

- `datastore.Get` -> `nds.Get`
- `datastore.Put` -> `nds.Put`
- `datastore.Delete` -> `nds.Delete`
- `datastore.RunInTransaction` -> `nds.RunInTransaction`
