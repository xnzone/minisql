package bpt

type BPTreeNode struct {
	IsLeaf    bool          // 是否为叶子节点
	Degree    int           // 节点度数
	Cnt       int           // 节点汇总当前的key数量
	Parent    *BPTreeNode   // 父节点
	Sibling   *BPTreeNode   // 兄弟节点
	Keys      []string      // 叶子和非叶子节点都有的搜索码的值,统一用string
	KeyOffset []int         // 叶子节点的搜索码对应的记录的序号
	Children  []*BPTreeNode // 非叶子节点的子节点
}

func NewBPTreeNode(degree int, isLeaf bool) *BPTreeNode {
	return &BPTreeNode{
		IsLeaf:    isLeaf,
		Degree:    degree,
		Cnt:       0,
		Parent:    nil,
		Sibling:   nil,
		Children:  make([]*BPTreeNode, degree+1, degree+1),
		Keys:      make([]string, degree, degree),
		KeyOffset: make([]int, degree, degree),
	}
}

func (b *BPTreeNode) Search(key string) (bool, int) {
	if b.Cnt == 0 {
		return false, 0
	}
	if key < b.Keys[0] {
		return false, 0
	}
	if key > b.Keys[b.Cnt-1] {
		return false, b.Cnt
	}
	return b.binarySearch(key)
}

func (b *BPTreeNode) binarySearch(key string) (bool, int) {
	left, right, pos := 0, b.Cnt-1, 0
	for left <= right {
		pos = left + (right-left)/2
		if b.Keys[pos] < key {
			left = pos + 1
		} else {
			right = pos - 1
		}
	}
	index := left
	return b.Keys[index] == key, index
}

func (b *BPTreeNode) split() (string, *BPTreeNode) {
	var key string
	newNode := NewBPTreeNode(b.Degree, b.IsLeaf)
	minimal := (b.Degree - 1) / 2
	if b.IsLeaf {
		// 叶子节点分keys，叶子的元素数量为[n/2]~[n-1]
		key = b.Keys[minimal+1]
		for i := minimal + 1; i < b.Degree; i++ {
			newNode.Keys[i-minimal-1] = b.Keys[i]
			newNode.KeyOffset[i-minimal-1] = b.KeyOffset[i]
		}
		newNode.Sibling = b.Sibling
		b.Sibling = newNode
		b.Cnt = minimal + 1
	} else {
		// 非叶子节点分children和keys，非叶子节点的元素数量为[n/2]~[n]
		key = b.Keys[minimal]
		for i := minimal + 1; i <= b.Degree; i++ {
			newNode.Children[i-minimal-1] = b.Children[i]
			b.Children[i].Parent = newNode
			b.Children[i] = nil
		}
		for i := minimal + 1; i < b.Degree; i++ {
			newNode.Keys[i-minimal-1] = b.Keys[i]
		}
		b.Cnt = minimal
	}
	newNode.Parent = b.Parent
	newNode.Cnt = b.Degree - minimal - 1
	return key, newNode
}
