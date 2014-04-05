package nds_test

import (
    "strconv"
    "appengine"
    "testing"
    "appengine/datastore"
    "appengine/aetest"
    "github.com/qedus/nds"
)

func TestGetMulti(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    type testEntity struct {
        Val int
    }

    // Test no such entity.
    for _, count := range []int{ 999, 1000, 1001, 5000, 5001} {

        keys := []*datastore.Key{}
        entities := []*testEntity{}
        for i := 0; i < count; i++ {
            keys = append(keys, 
            datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil))
            entities = append(entities, &testEntity{})
        }

        err = nds.GetMulti(c, keys, entities)
        if me, ok := err.(appengine.MultiError); ok {
            if len(me) != count {
                t.Fatal("multi error lenght incorrect")
            }
            for _, e := range me {
                if e != datastore.ErrNoSuchEntity {
                    t.Fatal(e)
                }
            }
        }
    }

    // Test entity saved.
    if _, err := datastore.Put(c, datastore.NewKey(c, "Test", "3", 0, nil),
    &testEntity{3}); err != nil {
        t.Fatal(err)
    }
    for _, count := range []int{ 999, 1000, 1001, 5000, 5001} {

        keys := []*datastore.Key{}
        entities := []*testEntity{}
        for i := 0; i < count; i++ {
            keys = append(keys, 
            datastore.NewKey(c, "Test", strconv.Itoa(i), 0, nil))
            entities = append(entities, &testEntity{})
        }

        err = nds.GetMulti(c, keys, entities)
        if me, ok := err.(appengine.MultiError); ok {
            if len(me) != count {
                t.Fatal("multi error lenght incorrect")
            }
            for i, e := range me {
                if i == 3 {
                    if e != nil {
                        t.Fatal("shoud be nil error")
                    }
                } else {
                    if e != datastore.ErrNoSuchEntity {
                        t.Fatal(e)
                    }
                }
            }
        }
    }
}
