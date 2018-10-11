/*
Package nds is a Go datastore API for Google Cloud Datastore that caches datastore
calls in a cache in a strongly consistent manner. This often has the effect
of making your app faster as cache access is often 10x faster than datastore
access. It can also make your app cheaper to run as cache calls are typically cheaper.

This package goes to great lengths to ensure that stale datastore values are
never returned to clients, i.e. the caching layer is strongly consistent.
It does this by using a similar strategy to Python's ndb. However, this
package fixes a couple of subtle edge case bugs that are found in ndb. See
http://goo.gl/3ByVlA for one such bug.

There are currently no known consistency issues with the caching strategy
employed by this package.

Use

Package nds' Client is used exactly the same way as the cloud.google.com/go/datastore.Client for
implemented calls. Ensure that you change all your datastore client Get, Put, Delete and
RunInTransaction function calls to use the nds client and Transaction type when converting your
own code. The one caveat with transactions is when running queries, there is a helper function for
adding the transaction to a datastore.Query.

If you mix datastore and nds API calls then you are liable to get stale cache. Currently, Mutations
are not supported (but are incoming!)

Implement your own cache

You can implement your own nds.Cacher and use it in place of the cache backends provided by this package.
The cache backends offered by Google such as AppEngine's Memcache and Cloud Memorystore (redis) are available
via this package and can be used as references when adding your own.
*/
package nds
