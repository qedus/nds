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

	*datastore.Client
}

// ClientOption is an option for a nds Client.
// Inspired by Google's api/option package.
type ClientOption func(*Config)

type Config struct {
	// DatatstoreClient is the prepared client to use for interacting with
	// Google Cloud Datastore with.
	datastoreClient *datastore.Client
	// OnError is called for every internal error that doesn't return to the
	// caller but maybe useful to capture for logging/debugging/reporting
	// purposes. For example, to keep the original nds.v1 behavior of logging
	// warnings in AppEngine Standard, this could be:
	// func(ctx context.Context, err error) { log.Warningf(ctx, "%s", err) }
	// By default this will log the error using the standard go log package.
	onErrorFn OnErrorFunc
}

func WithDatastoreClient(ds *datastore.Client) ClientOption {
	return func(c *Config) {
		c.datastoreClient = ds
	}
}

func WithOnErrorFunc(f OnErrorFunc) ClientOption {
	return func(c *Config) {
		c.onErrorFn = f
	}
}

// NewClient will return an nds.Client that can be used exactly like a datastore.Client but will
// transparently use the cache configuration provided to cache requests when it can.
func NewClient(ctx context.Context, cacher Cacher, opts ...ClientOption) (*Client, error) {
	var cfg Config

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.datastoreClient == nil {
		// Default datastore.Client
		if ds, err := datastore.NewClient(ctx, ""); err != nil {
			return nil, err
		} else {
			cfg.datastoreClient = ds
		}
	}

	return &Client{
		cacher:    cacher,
		onErrorFn: cfg.onErrorFn,
		Client:    cfg.datastoreClient,
	}, nil
}

func (c *Client) onError(ctx context.Context, err error) {
	if c.onErrorFn != nil {
		c.onErrorFn(ctx, err)
		return
	}
	log.Println(err)
}
