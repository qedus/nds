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

	// TODO: Client is exported since we embedded datastore.Client - fix this
	*datastore.Client
}

// ClientOption is an option for a nds Client.
// Inspired by Google's api/option package.
type ClientOption func(*Client)

func WithDatastoreClient(ds *datastore.Client) ClientOption {
	return func(c *Client) {
		c.Client = ds
	}
}

// WithOnErrorFunc sets up an OnErrorFunc to be called for every internal
// error that doesn't return to the caller but maybe useful to capture for
// logging/debugging/reporting purposes. For example, to keep the original
// nds.v1 behavior of logging warnings in AppEngine Standard, this could be:
// func(ctx context.Context, err error) { log.Warningf(ctx, "%s", err) }
// By default this will log the error using the standard go log package.
func WithOnErrorFunc(f OnErrorFunc) ClientOption {
	return func(c *Client) {
		c.onErrorFn = f
	}
}

// NewClient will return an nds.Client that can be used exactly like a datastore.Client but will
// transparently use the cache configuration provided to cache requests when it can.
func NewClient(ctx context.Context, cacher Cacher, opts ...ClientOption) (*Client, error) {
	client := &Client{
		cacher: cacher,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.Client == nil {
		// Default datastore.Client
		if ds, err := datastore.NewClient(ctx, ""); err != nil {
			return nil, err
		} else {
			client.Client = ds
		}
	}

	return client, nil
}

func (c *Client) onError(ctx context.Context, err error) {
	if c.onErrorFn != nil {
		c.onErrorFn(ctx, err)
		return
	}
	log.Println(err)
}
