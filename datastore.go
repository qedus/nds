package nds

import (
    "errors"
	"appengine"
	"appengine/datastore"
    "reflect"
)

const (
    // multiLimit is the App Engine datastore limit for the number of entities
    // that can be PutMulti or GetMulti in one call.
    multiLimit = 1000
)

func Get(c appengine.Context, key *datastore.Key, dst interface{}) error {
	return datastore.Get(c, key, dst)
}

// GetMulti works just like datastore.GetMulti except it calls
// datastore.GetMulti as many times as required to complete a request of over
// 1000 entities.
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
    errs := make([]error, 0, p+1)
    for i := 0; i < p; i++ {
        keySlice := keys[i*multiLimit:(i+1)*multiLimit]
        dstSlice := v.Slice(i*multiLimit, (i+1)*multiLimit)
        errs = append(errs, datastore.GetMulti(c, keySlice, dstSlice))
    }

    keySlice := keys[p*multiLimit:len(keys)]
    dstSlice := v.Slice(p*multiLimit, len(keys))
    errs = append(errs, datastore.GetMulti(c, keySlice, dstSlice))

    // Make sure error is nil or appengine.MultiError.
    errsNil := true
    for _, err := range errs {
        if err != nil {
            errsNil = false
        } 
        if _, ok := err.(appengine.MultiError); !ok {
            return err
        }
    }

    // Easy case. All errors are nil.
    if errsNil {
        return nil
    }

    // At this point some errors are nil and some are appengine.MultiError. We
    // must group them into one long appengine.MultiError.
    groupedErrs := make(appengine.MultiError, 0, len(keys))
    for i, err := range errs {
        if err == nil {
            r := multiLimit
            if i == len(errs) - 1 {
                r = len(keys) % multiLimit
            }
            for j := 0; j < r; j++ {
                groupedErrs = append(groupedErrs, nil)
            }
        } else if me, ok := err.(appengine.MultiError); ok {
            groupedErrs = append(groupedErrs, me...)
        }
    }
    return groupedErrs
}

func Put(c appengine.Context, 
    key *datastore.Key, src interface{}) (*datastore.Key, error) {
	return datastore.Put(c, key, src)
}
