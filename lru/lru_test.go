package lru

import (
	"fmt"
	"testing"
	"time"
)

type Block struct {
	FileName string
	Bid      int
	Dirty    bool
	RefCnt   int
}

func TestLRU(t *testing.T) {
	cache := NewLRU(10, time.Second*5)
	b := &Block{
		FileName: "table",
		Bid:      0,
		Dirty:    false,
		RefCnt:   0,
	}
	cache.Set(key(b.FileName, b.Bid), b, 0)
	b.RefCnt = 10
	val, _, _ := cache.Get(key(b.FileName, b.Bid))
	t.Log(val)
}

func key(fileName string, bid int) string {
	return fmt.Sprintf("%s-%d", fileName, bid)
}
