package bpt

import "fmt"

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

type Nodemap struct {
	Index int
	Node  *BPTreeNode
}

func NewBPTree(fileName string, sizeOfKey int, degree int) *BPTree {
	// 因为后面有拿children的数据，如果degree太小的话，remove时候会导致sibling为nil，所以这里degree必须要大于3
	if degree < 3 {
		degree = 3
	}
	bpt := &BPTree{
		FileName:  fileName,
		SizeOfKey: sizeOfKey,
		Degree:    degree,
		KeyCount:  0,
		NodeCount: 0,
		Level:     0,
		Root:      nil,
		Head:      nil,
	}
	initRoot(bpt)
	return bpt
}

func initRoot(bpt *BPTree) {
	bpt.Root = NewBPTreeNode(bpt.Degree, true)
	bpt.Head = bpt.Root
	bpt.KeyCount = 0
	bpt.Level = 1
	bpt.NodeCount = 1
}

func (b *BPTree) GetHeadNode() *BPTreeNode {
	return b.Head
}

func (b *BPTree) FindKeyFromNode(node *BPTreeNode, key string, res *Nodemap) bool {
	keyExist, index := node.Search(key)
	if keyExist {
		// 找到了key
		if node.IsLeaf {
			// 叶子节点
			res.Index = index
		} else {
			// 非叶子节点，往下层，一定是最左边那个
			node = node.Children[index+1]
			for !node.IsLeaf {
				node = node.Children[0]
			}
			res.Index = 0
		}
		res.Node = node
		return true
	} else {
		// 没找到key
		if node.IsLeaf {
			res.Index = index
			res.Node = node
			return false
		} else {
			return b.FindKeyFromNode(node.Children[index], key, res)
		}
	}
}

// 返回的是key所对应的keyoffset
func (b *BPTree) Find(key string) int {
	if b.Root == nil {
		return -1
	}
	res := &Nodemap{}
	if b.FindKeyFromNode(b.Root, key, res) {
		return res.Node.KeyOffset[res.Index]
	}
	return -1
}

// 返回key所在的node
func (b *BPTree) FindNode(key string) *Nodemap {
	if b.Root == nil {
		return nil
	}
	res := &Nodemap{}
	if b.FindKeyFromNode(b.Root, key, res) {
		return res
	}
	return nil
}

func (b *BPTree) Insert(key string, offset int) bool {
	res := &Nodemap{}
	if b.Root == nil {
		initRoot(b)
	}
	if b.FindKeyFromNode(b.Root, key, res) {
		fmt.Println("insert duplicate key")
		return false
	}
	res.Node.AddLeaf(key, offset)
	// 达到度数时，需要分裂
	if res.Node.Cnt == b.Degree {
		b.cascadeInsert(res.Node)
	}
	b.KeyCount++
	return true
}

func (b *BPTree) Remove(key string) bool {
	res := &Nodemap{}
	if b.Root == nil {
		fmt.Println("Dequeuing empty BPTree!")
		return false
	}
	if !b.FindKeyFromNode(b.Root, key, res) {
		fmt.Println("Key not found!")
		return false
	}
	if res.Node.IsRoot() { //要删的结点是根的话
		res.Node.RemoveAt(res.Index) //删除根结点中的一个index
		b.KeyCount--
		return b.cascadeDelete(res.Node)
	} else { //要删的结点不是根
		if res.Index == 0 && b.Head != res.Node {
			// cascadingly update parent node
			currentParent := res.Node.Parent
			keyFound, index := currentParent.Search(key)
			for !keyFound { //找到第一个有被删掉的那个node的key的非叶子的父亲
				if currentParent.Parent == nil {
					break
				}
				currentParent = currentParent.Parent
				keyFound, index = currentParent.Search(key)
			}
			currentParent.Keys[index] = res.Node.Keys[1]
			res.Node.RemoveAt(res.Index)
			b.KeyCount--
			return b.cascadeDelete(res.Node)
		} else {
			res.Node.RemoveAt(res.Index)
			b.KeyCount--
			return b.cascadeDelete(res.Node)
		}
	}
}

func (b *BPTree) cascadeInsert(node *BPTreeNode) {
	key, sibling := node.split()
	b.NodeCount++
	if node.IsRoot() {
		root := NewBPTreeNode(b.Degree, false)
		b.Level++
		b.NodeCount++
		b.Root = root
		node.Parent = root
		sibling.Parent = root
		root.AddNode(key)
		root.Children[0] = node
		root.Children[1] = sibling
	} else {
		parent := node.Parent
		index := parent.AddNode(key)
		parent.Children[index+1] = sibling
		sibling.Parent = parent
		if parent.Cnt == b.Degree {
			b.cascadeInsert(parent)
		}
	}
}

func (b *BPTree) cascadeDelete(node *BPTreeNode) bool {
	minimal, minimalBranch := b.Degree/2, (b.Degree-1)/2
	if (node.IsLeaf && node.Cnt >= minimal) || (node.IsRoot() && node.Cnt > 0) || (!node.IsLeaf && !node.IsRoot() && node.Cnt >= minimal) {
		return true // no need to update
	}
	if node.IsRoot() {
		if b.Root.IsLeaf { //根是叶子，整棵树就一个node
			// tree completely removed
			b.Root = nil
			b.Head = nil
		} else {
			// reduce level by one
			b.Root = node.Children[0]
			b.Root.Parent = nil
		}
		node = nil
		b.NodeCount--
		b.Level--
		return true
	}
	var currentParent, sibling *BPTreeNode
	currentParent = node.Parent

	index := 0
	if node.IsLeaf {
		// merge if it is leaf node
		_, index = currentParent.Search(node.Keys[0])
		if currentParent.Children[0] != node && currentParent.Cnt == index+1 {
			// rightest, also not first, merge with left sibling
			sibling = currentParent.Children[index]
			if sibling.Cnt > minimal {
				// transfer rightest of left to the leftest to meet the requirement
				return b.deleteLeafLL(node, currentParent, sibling, index)
			} else {
				// have to merge and cascadingly merge
				return b.deleteLeafLR(node, currentParent, sibling, index)
			}
		} else {
			// can merge with right brother
			if currentParent.Children[0] == node {
				// on the leftest
				sibling = currentParent.Children[1]
			} else {
				// normally
				sibling = currentParent.Children[index+2]
			}
			if sibling.Cnt > minimal {
				// add the leftest of sibling to the right
				return b.deleteLeafRL(node, currentParent, sibling, index)
			} else {
				// merge and cascadingly delete
				return b.deleteLeafRR(node, currentParent, sibling, index)
			}
		}
	} else {
		// merge if it is branch node
		_, index = currentParent.Search(node.Children[0].Keys[0])
		if currentParent.Children[0] != node && currentParent.Cnt == index+1 {
			// can only be updated with left sibling
			sibling = currentParent.Children[index]
			if sibling.Cnt > minimalBranch {
				// add rightest key to the first node to avoid cascade operation
				return b.deleteBranchLL(node, currentParent, sibling, index)
			} else {
				// delete this and merge
				return b.deleteBranchLR(node, currentParent, sibling, index)
			}
		} else {
			// update with right sibling
			if currentParent.Children[0] == node {
				sibling = currentParent.Children[1]
			} else {
				sibling = currentParent.Children[index+2]
			}
			if sibling.Cnt > minimalBranch {
				// add first key of sibling to the right
				return b.deleteBranchRL(node, currentParent, sibling, index)
			} else {
				// merge the sibling to current node
				return b.deleteBranchRR(node, currentParent, sibling, index)
			}
		}
	}
}

func (b *BPTree) deleteLeafLL(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	// transfer rightest of left to the leftest to meet the requirement
	for i := node.Cnt; i > 0; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.KeyOffset[i] = node.KeyOffset[i-1]
	}
	node.Keys[0] = sibling.Keys[sibling.Cnt-1]
	node.KeyOffset[0] = sibling.KeyOffset[sibling.Cnt-1]
	sibling.RemoveAt(sibling.Cnt - 1)
	node.Cnt++
	parent.Keys[index] = node.Keys[0]
	return true
}

func (b *BPTree) deleteLeafLR(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	//merge and cascadingly merge
	parent.RemoveAt(index)
	for i := 0; i < node.Cnt; i++ {
		sibling.Keys[i+sibling.Cnt] = node.Keys[i]
		sibling.KeyOffset[i+sibling.Cnt] = node.KeyOffset[i]
	}
	sibling.Cnt += node.Cnt
	sibling.Sibling = node.Sibling
	node = nil
	b.NodeCount--
	return b.cascadeDelete(parent)
}

func (b *BPTree) deleteLeafRL(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	// add the leftest of sibling to the right
	node.Keys[node.Cnt] = sibling.Keys[0]
	node.KeyOffset[node.Cnt] = sibling.KeyOffset[0]
	node.Cnt++
	sibling.RemoveAt(0)
	if parent.Children[0] == node {
		// if it is leftest, change key at index zero
		parent.Keys[0] = sibling.Keys[0]
	} else {
		// or next sibling should be updated
		parent.Keys[index+1] = sibling.Keys[0]
	}
	return true
}

func (b *BPTree) deleteLeafRR(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	//merge and cascadingly delete
	for i := 0; i < sibling.Cnt; i++ {
		node.Keys[node.Cnt+i] = sibling.Keys[i]
		node.KeyOffset[node.Cnt+i] = sibling.KeyOffset[i]
	}
	if node == parent.Children[0] {
		parent.RemoveAt(0) // if leftest, merge with first sibling
	} else {
		parent.RemoveAt(index + 1) // or merge with next
	}
	node.Cnt += sibling.Cnt
	node.Sibling = sibling.Sibling
	sibling = nil
	b.NodeCount--
	return b.cascadeDelete(parent)
}

func (b *BPTree) deleteBranchLL(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	// add rightest key to the first node to avoid cascade operation
	node.Children[node.Cnt+1] = node.Children[node.Cnt]
	for i := node.Cnt; i > 0; i-- {
		node.Children[i] = node.Children[i-1]
		node.Keys[i] = node.Keys[i-1]
	}
	node.Children[0] = sibling.Children[sibling.Cnt]
	node.Keys[0] = parent.Keys[index]
	parent.Keys[index] = sibling.Keys[sibling.Cnt-1]
	node.Cnt++
	sibling.Children[sibling.Cnt].Parent = node
	sibling.RemoveAt(sibling.Cnt - 1)
	return true
}

func (b *BPTree) deleteBranchLR(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	// delete this and merge
	sibling.Keys[sibling.Cnt] = parent.Keys[index] // add one node
	parent.RemoveAt(index)
	sibling.Cnt++
	for i := 0; i < node.Cnt; i++ {
		node.Children[i].Parent = sibling
		sibling.Children[sibling.Cnt+i] = node.Children[i]
		sibling.Keys[sibling.Cnt+i] = node.Keys[i]
	}
	// rightest children
	sibling.Children[sibling.Cnt+node.Cnt] = node.Children[node.Cnt]
	sibling.Children[sibling.Cnt+node.Cnt].Parent = sibling
	sibling.Cnt += node.Cnt
	node = nil
	b.NodeCount--
	return b.cascadeDelete(parent)
}
func (b *BPTree) deleteBranchRL(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	sibling.Children[0].Parent = node
	node.Children[node.Cnt+1] = sibling.Children[0]
	node.Keys[node.Cnt] = sibling.Children[0].Keys[0]
	node.Cnt++

	if node == parent.Children[0] {
		parent.Keys[0] = sibling.Keys[0]
	} else {
		parent.Keys[index+1] = sibling.Keys[0]
	}

	sibling.Children[0] = sibling.Children[1]
	sibling.RemoveAt(0)
	return true
}
func (b *BPTree) deleteBranchRR(node *BPTreeNode, parent *BPTreeNode, sibling *BPTreeNode, index int) bool {
	node.Keys[node.Cnt] = parent.Keys[index]
	if node == parent.Children[0] {
		parent.RemoveAt(0)
	} else {
		parent.RemoveAt(index + 1)
	}
	node.Cnt++
	for i := 0; i < sibling.Cnt; i++ {
		sibling.Children[i].Parent = node
		node.Children[node.Cnt+i] = sibling.Children[i]
		node.Keys[node.Cnt+i] = sibling.Keys[i]
	}
	sibling.Children[sibling.Cnt].Parent = node
	node.Children[node.Cnt+sibling.Cnt] = sibling.Children[sibling.Cnt]
	node.Cnt += sibling.Cnt
	sibling = nil
	b.NodeCount--
	return b.cascadeDelete(parent)
}
