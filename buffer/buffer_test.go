package buffer

import "testing"

func TestNewBufferManager(t *testing.T) {
	bm := NewBufferManager()
	fileName := "dbtest"
	bid := bm.BRead(fileName, 1)
	data := bm.BAddr(bid)
	t.Log(data)
	t.Log(string(bm.blocks[bid].Data))
	copy(data, "this is test string!\n")
	t.Log(data)
	t.Log(string(bm.blocks[bid].Data))
	bm.BWrite(bid)
	bm.BRelease(bid)
	t.Log(string(bm.blocks[bid].Data))

	bid = bm.BRead(fileName, 1)

}
