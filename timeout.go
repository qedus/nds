package nds

import (
	"appengine"
	"time"
)

// Timeout returns a context that will set the timeout duration for datastore
// RPC calls. The default timeout is 5 seconds and can be set to a maximum of
// 60 seconds.
// Use this packages Timeout function instead of appengine.Timeout to ensure
// transactions works with nds.
func Timeout(c appengine.Context, d time.Duration) appengine.Context {
	if ndsc, ok := c.(*context); ok {
		return &context{
			Context:       appengine.Timeout(ndsc.Context, d),
			inTransaction: ndsc.inTransaction,
		}
	}
	return appengine.Timeout(c, d)
}
