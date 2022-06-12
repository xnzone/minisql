package buffer

import (
	"fmt"
	"github.com/xnzone/minisql/lru"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	blockCache *lru.LRU // block使用lru,key是fileName + bid
)

func Init() {
	blockCache = lru.NewLRU(BlockNum, time.Second*2, lru.TimeoutFunc(timeoutFunc))
}

func timeoutFunc(key, value interface{}) {
	strs := strings.Split(key.(string), "-")
	fileName := strs[0]
	bid, _ := strconv.ParseInt(strs[1], 10, 64)
	BRelease(fileName, int(bid))
}

func BAddr(fileName string, bid int) []byte {
	bp := bread(fileName, bid)
	return bp.Data
}

func BRelease(fileName string, bid int) {
	key := fileNameKey(fileName, bid)
	val, _, exist := blockCache.Get(key)
	if !exist {
		return
	}
	bp := val.(*Block)
	if bp == nil {
		return
	}
	if bp.RefCnt <= 0 || bp.IsPinned {
		return
	}

	if bp.IsDirty {
		bwrite(bp)
		bp.IsDirty = false
	}
	bp.RefCnt--
}

func BFlush(fileName string) {
	blockm := blockCache.All()
	for _, v := range blockm {
		bp := v.(*Block)
		if bp == nil {
			continue
		}
		if bp.FileName == fileName {
			_ = blockCache.Delete(fileNameKey(bp.FileName, bp.Bid))
		}
	}
}

func BRemove(fileName string) {
	_ = os.Remove(fileNameData(fileName))
}

func bwrite(bp *Block) bool {
	if bp == nil || bp.IsDirty == false {
		return true
	}
	fname := fileNameData(bp.FileName)
	fd, err := os.OpenFile(fname, os.O_RDWR, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("open file err: ", err, ", filename: ", fname)
		return false
	}
	_, err = fd.Seek(int64(bp.Bid*BlockSize), 0)
	if err != nil {
		fmt.Println("seek file err: ", err, ", filename: ", fname)
		return false
	}
	_, err = fd.Write(bp.Data)
	if err != nil {
		fmt.Println("write file err: ", err, ", filename: ", fname)
		return false
	}
	return true
}

func bread(fileName string, bid int) *Block {
	key := fileNameKey(fileName, bid)
	val, _, exist := blockCache.Get(key)
	if exist {
		bp := val.(*Block)
		if bp != nil && bp.UpToDate {
			return bp
		}
	}
	bp := NewBlock()
	bp.Bid = bid
	bp.FileName = fileName
	blockCache.Set(key, bp, 0)

	fname := fileNameData(fileName)
	fd, err := os.OpenFile(fname, os.O_RDONLY|os.O_CREATE, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("open file err: ", err, ", filename: ", fname)
		return bp
	}
	_, err = fd.Seek(int64(bp.Bid*BlockSize), 0)
	if err != nil {
		fmt.Println("seek file err: ", err, ", filename: ", fname)
		return bp
	}
	_, err = fd.Read(bp.Data)
	if err != nil && err != io.EOF {
		fmt.Println("read file err: ", err, ", filename: ", fname)
		return bp
	}
	if err == io.EOF {
		_, _ = fd.WriteString("")
	}
	bp.UpToDate = true
	return bp
}

func fileNameKey(fileName string, bid int) string {
	return fmt.Sprintf("%s-%d", fileName, bid)
}

func fileNameData(fileName string) string {
	return fmt.Sprintf("%s.data", fileName)
}
