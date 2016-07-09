/*
Package nds is a Go datastore API for Google App Engine that caches datastore
calls in memcache in a strongly consistent manner. This often has the effect
of making your app faster as memcache access is often 10x faster than datastore
access. It can also make your app cheaper to run as memcache calls are free.

This package goes to great lengths to ensure that stale datastore values are
never returned to clients, i.e. the caching layer is strongly consistent.
It does this by using a similar strategy to Python's ndb. However, this
package fixes a couple of subtle edge case bugs that are found in ndb. See
http://goo.gl/3ByVlA for one such bug.

There are currently no known consistency issues with the caching strategy
employed by this package.

Use

Package nds is used exactly the same way as appeninge/datastore. Ensure that
you change all your datastore Get, Put, Delete and RunInTransaction function
calls to use nds when converting your own code.

If you mix appengine/datastore and nds API calls then you are liable to get
stale cache.

Converting Legacy Code

To convert legacy code you will need to find and replace all invocations of
datastore.Get, datastore.Put, datastore.Delete, datastore.RunInTransaction with
nds.Get, nds.Put, nds.Delete and nds.RunInTransaction respectively.
*/
package nds
