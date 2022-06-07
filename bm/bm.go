package bm

var (
	bmgr *BufferManager
)

func Init() {
	bmgr = NewBufferManager()
}

func BRead(fileName string, offset int) int {
	return bmgr.BRead(fileName, offset)
}

func BAddr(bid int) []byte {
	return bmgr.BAddr(bid)
}

func BFlush(fileName string) {
	bmgr.BFlush(fileName)
}

func BRelease(bid int) {
	bmgr.BRelease(bid)
}

func BWrite(bid int) bool {
	return bmgr.BWrite(bid)
}
