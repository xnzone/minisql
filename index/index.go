package index

import (
	"fmt"
	"github.com/xnzone/minisql/bptree"
	"github.com/xnzone/minisql/buffer"
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/util"
	"go/constant"
	"strings"
)

var (
	treeMap map[string]*bptree.BPTree
)

func Init() {
	treeMap = make(map[string]*bptree.BPTree)
}

func CreateIndex(table *database.Table, indexName, columnName string) {
	if treeMap[indexName] != nil {
		fmt.Println("This table already had an index")
		return
	}
	indexCol := table.IndexOfColumn(columnName)
	size := table.Columns[indexCol].Size()
	numRecord := table.Rib()
	sum := 1
	for k := 0; k < indexCol; k++ {
		sum += table.Columns[k].Size()
	}
	tree := bptree.NewBPTree(indexName, size, 4096/(size+4))
	treeMap[indexName] = tree
	tree.KeyCount = 0
	tree.Level = 1
	tree.NodeCount = 1
	tree.Root = bptree.NewBPTreeNode(tree.Degree, true)
	tree.Head = tree.Root

	for i := 0; i < table.BlockCnt; i++ {
		data := buffer.BAddr(table.TableName, i)
		offset := 0
		for j := 0; j < numRecord; j++ {
			if data[offset] == 0 {
				offset += table.Size() + 1
				continue
			} else {
				offset += sum
				col := table.Columns[indexCol]
				var key string
				switch col.Field {
				case constant.Int:
					va := util.Byte2Int(data[offset : offset+col.Size()])
					key = fmt.Sprintf("%d", va)
				case constant.Float:
					va := util.Byte2Float(data[offset : offset+col.Size()])
					key = fmt.Sprintf("%f", va)
				default:
					va := make([]byte, col.Size())
					copy(va, data[offset:offset+col.Size()])
					key = strings.TrimSpace(string(va))
				}
				tree.Insert(key, i*numRecord+j)
				offset = offset + table.Size() + 1 - sum
			}
		}
	}
}

func DropIndex(table *database.Table, indexName, columnName string) {
	if table == nil {
		return
	}
	tree, ok := treeMap[indexName]
	if !ok || tree == nil {
		return
	}
	tree.CascadeDelete(tree.Root)
	delete(treeMap, indexName)
}

func InsertIndex(table *database.Table, indexName, columnName string, val constant.Value, offset int) {
	if table == nil {
		return
	}
	tree, ok := treeMap[indexName]
	if !ok || tree == nil {
		return
	}
	key := treeKey(table, columnName, val)
	tree.Insert(key, offset)
}
func DeleteIndex(table *database.Table, indexName, columnName string, val constant.Value) {
	if table == nil {
		return
	}
	tree, ok := treeMap[indexName]
	if !ok || tree == nil {
		return
	}
	key := treeKey(table, columnName, val)
	tree.Remove(key)
}

func FindIndex(table *database.Table, indexName, columnName string, val constant.Value) int {
	if table == nil {
		return -1
	}
	tree, ok := treeMap[indexName]
	if !ok || tree == nil {
		return -1
	}
	key := treeKey(table, columnName, val)
	return tree.Find(key)
}

func treeKey(table *database.Table, columnName string, val constant.Value) string {
	if table == nil {
		return ""
	}
	idx := table.IndexOfColumn(columnName)
	attr := table.Columns[idx]
	switch attr.Field {
	case constant.Int:
		va, _ := constant.Int64Val(val)
		return fmt.Sprintf("%d", va)
	case constant.Float:
		va, _ := constant.Float64Val(val)
		return fmt.Sprintf("%f", va)
	default:
		va := constant.StringVal(val)
		return strings.TrimSpace(va)
	}
}
