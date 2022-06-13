package buffer

const (
	BlockSize = 0x1000
	BlockNum  = 0x1000
)

type Block struct {
	Data     []byte
	IsPinned bool
	IsDirty  bool
	UpToDate bool
	FileName string
	Bid      int
	RefCnt   int
}

func NewBlock(fileName string, bid int) *Block {
	return &Block{
		Data:     make([]byte, BlockSize, BlockSize),
		IsDirty:  false,
		IsPinned: false,
		UpToDate: false,
		FileName: fileName,
		Bid:      bid,
		RefCnt:   0,
	}
}
