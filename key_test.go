package nds

import (
    "testing"
    //"appengine/datastore"
    "appengine/aetest"
)

func TestNewKeys(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    incompleteKey := NewIncompleteKey(c, "Test", nil)
    if !incompleteKey.Incomplete() {
        t.Fatal("expecting incomplete key")
    }

    key := NewKey(c, "Test", "string", 0, nil)
    if key.Incomplete() {
        t.Fatal("expected complete key")
    }
}

func TestAncestorKeys(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    parentKey := NewKey(c, "Parent", "", 1, nil)
    incompleteKey := NewIncompleteKey(c, "Test", parentKey)
    if !incompleteKey.Incomplete() {
        t.Fatal("expecting incomplete key")
    }

    if !incompleteKey.Parent().Equal(parentKey) {
        t.Fatal("parent keys not equal")
    }

    key := NewKey(c, "Test", "string", 0, parentKey)
    if key.Incomplete() {
        t.Fatal("expected complete key")
    }

    if !key.Parent().Equal(parentKey) {
        t.Fatal("parent keys not equal")
    }
}

func TestEncodeDecode(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    key := NewKey(c, "Test", "string", 0, nil)
    encVal := key.Encode()

    decKey, err := DecodeKey(encVal)
    if err != nil {
        t.Fatal(err)
    }
    if !key.Equal(decKey) {
        t.Fatal("key != decKey")
    }
}

func TestGobEncodeDecode(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    key := NewKey(c, "Test", "string", 0, nil)
    encVal, err := key.GobEncode()
    if err != nil {
        t.Fatal(err)
    }

    decKey := &Key{}
    if err := decKey.GobDecode(encVal); err != nil {
        t.Fatal(err)
    }

    if !key.Equal(decKey) {
        t.Fatal("key != decKey")
    }
}

func TestJSONMarshalUnmarshal(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    key := NewKey(c, "Test", "string", 0, nil)
    encVal, err := key.MarshalJSON()
    if err != nil {
        t.Fatal(err)
    }

    decKey := &Key{}
    if err := decKey.UnmarshalJSON(encVal); err != nil {
        t.Fatal(err)
    }

    if !key.Equal(decKey) {
        t.Fatal("key != decKey")
    }
}

func TestAllocateIDs(t *testing.T) {
    c, err := aetest.NewContext(nil)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()

    parentKey := NewKey(c, "Parent", "string", 0, nil)
    low, high, err := AllocateIDs(c, "Test", parentKey, 20)
    if err != nil {
        t.Fatal(err)
    }
    if high - low != 20 {
        t.Fatal("high - low != 20", high, low)
    }
}
