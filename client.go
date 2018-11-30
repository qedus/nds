package nds

import (
	"context"
	"log"

	"cloud.google.com/go/datastore"
)

type OnErrorFunc func(ctx context.Context, err error)

type Client struct {
	cacher    Cacher
	onErrorFn OnErrorFunc
	ds        *datastore.Client
}

type Config struct {
	// Cacher is the caching backend you want to use when caching calls
	// to datastore.
	Cacher Cacher
	// DatatstoreClient is the prepared client to use for interacting with
	// Google Cloud Datastore with.
	DatastoreClient *datastore.Client
	// OnError is called for every internal error that doesn't return to the
	// caller but maybe useful to capture for logging/debugging/reporting
	// purposes. For example, to keep the original nds.v1 behavior of logging
	// warnings in AppEngine Standard, this could be:
	// func(ctx context.Context, err error) { log.Warningf(ctx, "%s", err) }
	// By default this will log the error using the standard go log package.
	OnError OnErrorFunc
}

// NewClient will return an nds.Client that can be used exactly like a datastore.Client but will
// transparently use the cache configuration provided to cache requests when it can.
func NewClient(ctx context.Context, cfg *Config) *Client {
	return &Client{
		cacher:    cfg.Cacher,
		ds:        cfg.DatastoreClient,
		onErrorFn: cfg.OnError,
	}
}

func (c *Client) onError(ctx context.Context, err error) {
	if c.onErrorFn != nil {
		c.onErrorFn(ctx, err)
		return
	}
	log.Println(err)
}
