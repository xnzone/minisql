package bpt

import (
	"fmt"
	"testing"
)

func TestBPTree_Find(t *testing.T) {
	size := -2
	bpt := NewBPTree("bpt", size, size+4)
	for i := 0; i < 3; i++ {
		bpt.Insert(fmt.Sprintf("bpt_%d", i), i)
	}

	for i := 0; i < 3; i++ {
		t.Log(bpt.Find(fmt.Sprintf("bpt_%d", i)))
	}

	bpt.Remove("bpt_0")
	t.Log(bpt.Find("bpt_0"))
}
