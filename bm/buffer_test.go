package bm

import "testing"

func TestNewBufferManager(t *testing.T) {
	bm := NewBufferManager()
	fileName := "dbtest"
	bid := bm.Bread(fileName, 1)
	data := bm.Baddr(bid)
	t.Log(string(bm.blocks[bid].Data))
	copy(data, "this is test string!\n")
	t.Log(data)
	t.Log(string(bm.blocks[bid].Data))
	bm.Bwrite(bid)
	bm.Brelease(bid)
	t.Log(string(bm.blocks[bid].Data))

	bid = bm.Bread(fileName, 1)

}
