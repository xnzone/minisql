package bpt

type BPTree struct {
	FileName  string
	Root      *BPTreeNode
	Head      *BPTreeNode
	SizeOfKey int
	Level     int
	KeyCount  int
	NodeCount int
	Degree    int
}
