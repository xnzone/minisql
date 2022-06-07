package im

import (
	"fmt"
	"github.com/xnzone/minisql/bpt"
	"github.com/xnzone/minisql/dm"
)

type IndexManager struct {
	TreeMap map[string]*bpt.BPTree
}

func NewIndexManager() *IndexManager {
	return &IndexManager{}
}

func (b *IndexManager) CreateIndex(indexName string, table *dm.Table, columnName string) {
	if b.TreeMap[indexName] != nil {
		fmt.Println("This table already had an index")
		return
	}
	indexCol := table.IndexOfColumn(columnName)
	size := table.Columns[indexCol].Size()
	//numRecord := table.Rib()
	sum := 1
	for k := 0; k < indexCol; k++ {
		sum += table.Columns[k].Size()
	}
	tree := bpt.NewBPTree(indexName, size, 4096/(size+4))
	b.TreeMap[indexName] = tree
	tree.KeyCount = 0
	tree.Level = 1
	tree.NodeCount = 1
	tree.Root = bpt.NewBPTreeNode(tree.Degree, true)
	tree.Head = tree.Root
}

func (b *IndexManager) FindIndex(indexName string, table *dm.Table, key string) int {
	return 0
}

func (b *IndexManager) InsertIndex(indexName string, table *dm.Table, key string, offset int) {

}

func (b *IndexManager) DeleteIndex(indexName string, table *dm.Table, key string) {

}

func (b *IndexManager) AlterIndex(indexName string, table *dm.Table, keyBefore string, keyAfter string, offset int) {

}

func (b *IndexManager) DropIndex(indexName string, table *dm.Table) {

}
