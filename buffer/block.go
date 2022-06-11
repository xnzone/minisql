package buffer

const (
	BlockSize = 0x1000
)

type Block struct {
	Data     []byte
	IsPinned bool
	IsDirty  bool
	UpToDate bool
	FileName string
	Offset   int
	RefCnt   int
}

func NewBlock() *Block {
	return &Block{
		Data:     make([]byte, BlockSize, BlockSize),
		IsDirty:  false,
		IsPinned: false,
		UpToDate: false,
		FileName: "",
		Offset:   0,
		RefCnt:   0,
	}
}
