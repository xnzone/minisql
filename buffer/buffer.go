package buffer

import (
	"fmt"
	"io"
	"os"
)

const (
	BlockNum = 0x1000
)

type BufferManager struct {
	blocks []*Block
}

func NewBufferManager() *BufferManager {
	bm := &BufferManager{}
	for i := 0; i < BlockNum; i++ {
		bm.blocks = append(bm.blocks, NewBlock())
	}
	return bm
}

func (b *BufferManager) PinBlock(bid int) {
	b.blocks[bid].IsPinned = true
}

func (b *BufferManager) DirtBlock(bid int) {
	b.blocks[bid].IsDirty = true
}

func (b *BufferManager) UnpinBlock(bid int) {
	b.blocks[bid].IsPinned = false
}

func (b *BufferManager) UndirtBlock(bid int) {
	b.blocks[bid].IsDirty = false
}

func (b *BufferManager) BRead(fileName string, offset int) int {
	bid := b.getBlk(fileName, offset)
	bp := b.blocks[bid]
	if bp.UpToDate {
		return bid
	}
	fname := getFileName(fileName)
	fd, err := os.OpenFile(fname, os.O_RDONLY|os.O_CREATE, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("open file err: ", err, ", filename: ", fileName)
		return -1
	}
	_, err = fd.Seek(int64(bp.Offset*BlockSize), 0)
	if err != nil {
		fmt.Println("seek file err: ", err, ", filename: ", fileName)
		return -1
	}
	_, err = fd.Read(bp.Data)
	if err != nil && err != io.EOF {
		fmt.Println("read file err: ", err, ", filename: ", fileName)
		return -1
	}
	if err == io.EOF {
		_, _ = fd.WriteString("")
	}
	bp.UpToDate = true
	return bid
}

func (b *BufferManager) BWrite(bid int) bool {
	bp := b.blocks[bid]
	fileName := getFileName(bp.FileName)
	fd, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("open file err: ", err, ", filename: ", fileName)
		return false
	}
	_, err = fd.Seek(int64(bp.Offset*BlockSize), 0)
	if err != nil {
		fmt.Println("seek file err: ", err, ", filename: ", fileName)
		return false
	}
	_, err = fd.Write(bp.Data)
	if err != nil {
		fmt.Println("write file err: ", err, ", filename: ", fileName)
		return false
	}
	return true
}

func (b *BufferManager) BRelease(bid int) {
	bp := b.blocks[bid]
	if bp.RefCnt <= 0 || bp.IsPinned {
		return
	}

	if bp.IsDirty {
		b.BWrite(bid)
		bp.IsDirty = false
	}
	bp.RefCnt--
}

func (b *BufferManager) BAddr(bid int) []byte {
	return b.blocks[bid].Data
}

func (b *BufferManager) BFlush(fileName string) {
	for i := 0; i < BlockNum; i++ {
		if b.blocks[i].FileName == fileName {
			if b.blocks[i].RefCnt != 0 {
				fmt.Println("Trying to flush referred blocks!")
				return
			}
			b.blocks[i].FileName = ""
			b.blocks[i].Offset = -1
			b.blocks[i].RefCnt = 0
			b.blocks[i].UpToDate = false
			b.blocks[i].IsDirty = false
			b.blocks[i].IsPinned = false
		}
	}
}

func (b *BufferManager) getBlk(fileName string, offset int) int {
	empty := -1

	for i := 0; i < BlockNum; i++ {
		if b.blocks[i].FileName == fileName && b.blocks[i].Offset == offset {
			b.blocks[i].RefCnt++
			return i
		}
		if empty == -1 && b.blocks[i].RefCnt == 0 {
			empty = i
		}
	}
	if empty == -1 {
		panic("No free blocks")
	}
	b.blocks[empty].FileName = fileName
	b.blocks[empty].Offset = offset
	b.blocks[empty].RefCnt = 1
	b.blocks[empty].IsDirty = false
	b.blocks[empty].IsPinned = false
	b.blocks[empty].UpToDate = false
	b.blocks[empty].Data = make([]byte, BlockSize, BlockSize)
	return empty
}

func getFileName(fileName string) string {
	return fmt.Sprintf("%s.data", fileName)
}
