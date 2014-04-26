package nds

import (
	"appengine"
	"appengine/datastore"
	"errors"
	"reflect"
	"sync"
)

const (
	// multiLimit is the App Engine datastore limit for the number of entities
	// that can be PutMulti or GetMulti in one call.
	multiLimit = 1000
)

var (
	// milMultiError is a convenience slice used to represent a nil error when
	// grouping errors in GetMulti.
	nilMultiError = make(appengine.MultiError, multiLimit)
)

// GetMulti works just like datastore.GetMulti except it removes the API limit
// of 1000 entities per request by calling datastore.GetMulti as many times as
// required to complete the request.
func GetMulti(c appengine.Context,
	keys []*datastore.Key, dst interface{}) error {

	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Slice {
		return errors.New("nds: dst is not a slice")
	}

	if len(keys) != v.Len() {
		return errors.New("nds: key and dst slices have different length")
	}

	if len(keys) == 0 {
		return nil
	}

	p := len(keys) / multiLimit
	errs := make([]error, p+1)
	wg := sync.WaitGroup{}
	for i := 0; i < p; i++ {
		index := i
		keySlice := keys[i*multiLimit : (i+1)*multiLimit]
		dstSlice := v.Slice(i*multiLimit, (i+1)*multiLimit)

		wg.Add(1)
		go func() {
			errs[index] = datastore.GetMulti(c, keySlice, dstSlice.Interface())
			wg.Done()
		}()
	}

	if len(keys)%multiLimit == 0 {
		errs = errs[:len(errs)-1]
	} else {
		keySlice := keys[p*multiLimit : len(keys)]
		dstSlice := v.Slice(p*multiLimit, len(keys))
		wg.Add(1)
		go func() {
			errs[p] = datastore.GetMulti(c, keySlice, dstSlice.Interface())
			wg.Done()
		}()
	}
	wg.Wait()

	// Quick escape if all errors are nil.
	errsNil := true
	for _, err := range errs {
		if err != nil {
			errsNil = false
		}
	}
	if errsNil {
		return nil
	}

	groupedErrs := make(appengine.MultiError, 0, len(keys))
	for _, err := range errs {
		if err == nil {
			groupedErrs = append(groupedErrs, nilMultiError...)
		} else if me, ok := err.(appengine.MultiError); ok {
			groupedErrs = append(groupedErrs, me...)
		} else {
			return err
		}
	}
	return groupedErrs[:len(keys)]
}
