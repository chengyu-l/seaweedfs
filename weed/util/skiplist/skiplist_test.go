package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

const (
	maxN = 10000
)

var (
	memStore = newMemStore()
)

func TestReverseInsert(t *testing.T) {
	list := NewSeed(100, memStore)

	list.Insert([]byte("zzz"), []byte("zzz"))
	list.Delete([]byte("zzz"))

	list.Insert([]byte("aaa"), []byte("aaa"))

	if list.IsEmpty() {
		t.Fail()
	}

}


func TestInsertAndFind(t *testing.T) {

	k0 := []byte("0")
	var list *SkipList

	var listPointer *SkipList
	listPointer.Insert(k0, k0)
	if _, _, ok, _ := listPointer.Find(k0); ok {
		t.Fail()
	}

	list = New(memStore)
	if _, _, ok, _ := list.Find(k0); ok {
		t.Fail()
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	// Test at the beginning of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN - i))
		list.Insert(key, key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN - i))
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}

	list = New(memStore)
	// Test at the end of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		list.Insert(key, key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}

	list = New(memStore)
	// Test at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		// println("insert", e)
		list.Insert(key, key)
	}
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		// println("find", e)
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}
	// println("print list")
	list.println()

}

func Element(x int) []byte {
	return []byte(strconv.Itoa(x))
}

func TestDelete(t *testing.T) {

	k0 := []byte("0")

	var list *SkipList

	// Delete on empty list
	list.Delete(k0)

	list = New(memStore)

	list.Delete(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	list.Insert(k0, k0)
	list.Delete(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	// Delete elements at the beginning of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i), Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(i))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New(memStore)
	// Delete elements at the end of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i), Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(maxN - i - 1))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New(memStore)
	// Delete elements at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		list.Insert(Element(e), Element(e))
	}
	for _, e := range rList {
		list.Delete(Element(e))
	}
	if !list.IsEmpty() {
		t.Fail()
	}
}

func TestNext(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i), Element(i))
	}

	smallest, _ := list.GetSmallestNode()
	largest, _ := list.GetLargestNode()

	lastNode := smallest
	node := lastNode
	for node != largest {
		node, _ = list.Next(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Key, lastNode.Key) <= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		prevNode, _ := list.Prev(node)
		nextNode, _ := list.Next(prevNode)
		if nextNode != node {
			t.Fail()
		}
		lastNode = node
	}

	if nextNode, _ := list.Next(largest); nextNode != smallest {
		t.Fail()
	}
}

func TestPrev(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i), Element(i))
	}

	smallest, _ := list.GetSmallestNode()
	largest, _ := list.GetLargestNode()

	lastNode := largest
	node := lastNode
	for node != smallest {
		node, _ = list.Prev(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Key, lastNode.Key) >= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		nextNode, _ := list.Next(node)
		prevNode, _ := list.Prev(nextNode)
		if prevNode != node {
			t.Fail()
		}
		lastNode = node
	}

	if prevNode, _ := list.Prev(smallest); prevNode != largest {
		t.Fail()
	}
}

func TestFindGreaterOrEqual(t *testing.T) {

	maxNumber := maxN * 100

	var list *SkipList
	var listPointer *SkipList

	// Test on empty list.
	if _, _, ok, _ := listPointer.FindGreaterOrEqual(Element(0)); ok {
		t.Fail()
	}

	list = New(memStore)

	for i := 0; i < maxN; i++ {
		list.Insert(Element(rand.Intn(maxNumber)), Element(i))
	}

	for i := 0; i < maxN; i++ {
		key := Element(rand.Intn(maxNumber))
		if _, v, ok, _ := list.FindGreaterOrEqual(key); ok {
			// if f is v should be bigger than the element before
			if v.Prev != nil && bytes.Compare(v.Prev.Key, key) >= 0 {
				fmt.Printf("PrevV: %s\n    key: %s\n\n", string(v.Prev.Key), string(key))
				t.Fail()
			}
			// v should be bigger or equal to f
			// If we compare directly, we get an equal key with a difference on the 10th decimal point, which fails.
			if bytes.Compare(v.Key, key) < 0 {
				fmt.Printf("v: %s\n    key: %s\n\n", string(v.Key), string(key))
				t.Fail()
			}
		} else {
			lastNode, _ := list.GetLargestNode()
			lastV := lastNode.GetValue()
			// It is OK, to fail, as long as f is bigger than the last element.
			if bytes.Compare(key, lastV) <= 0 {
				fmt.Printf("lastV: %s\n    key: %s\n\n", string(lastV), string(key))
				t.Fail()
			}
		}
	}

}

func TestChangeValue(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i), []byte("value"))
	}

	for i := 0; i < maxN; i++ {
		// The key only looks at the int so the string doesn't matter here!
		_, f1, ok, _ := list.Find(Element(i))
		if !ok {
			t.Fail()
		}
		err := list.ChangeValue(f1, []byte("different value"))
		if err != nil {
			t.Fail()
		}
		_, f2, ok, _ := list.Find(Element(i))
		if !ok {
			t.Fail()
		}
		if bytes.Compare(f2.GetValue(), []byte("different value")) != 0 {
			t.Fail()
		}
	}
}