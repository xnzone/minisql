package record

import (
	"fmt"
	"github.com/xnzone/minisql/buffer"
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/index"
	"github.com/xnzone/minisql/util"
	"go/constant"
	"go/token"
	"sort"
)

type piece struct {
	first  int
	second int
}

func Init() {
}

func CreateTable(table *database.Table) {
	if table == nil {
		return
	}
	block := buffer.BAddr(table.TableName, 0)
	buffer.BRelease(block)
	table.BlockCnt = 1
}

func DropTable(table *database.Table) {
	if table == nil {
		return
	}
	buffer.BFlush(table.TableName)
	buffer.BRemove(table.TableName)
	for _, col := range table.Columns {
		if col == nil || len(col.Index) <= 0 {
			continue
		}
		index.DropIndex(table, col.Index, col.ColumnName)
	}
}

func InsertRecord(table *database.Table, values []constant.Value) {
	if table == nil {
		return
	}
	attrs := table.Columns
	for i := 0; i < len(attrs); i++ {
		// 检查类型
		if !checkType(attrs[i], values[i]) {
			fmt.Printf("Insert failed, attribute %s type error!\n", attrs[i].ColumnName)
			return
		}
		// 检查唯一索引
		if attrs[i].IsUnique && !checkUnique(table, i, values[i]) {
			fmt.Printf("Insert failed, attribute %s type error!\n", attrs[i].ColumnName)
			return
		}
	}

	blockCnt := table.BlockCnt
	block := buffer.BAddr(table.TableName, blockCnt-1) // 插入到最后一页
	if block == nil {
		return
	}
	data := block.Data
	size := table.Size() + 1

	for offset := 0; offset < buffer.BlockSize; offset += size {
		if data[offset] == 0 && offset+size <= buffer.BlockSize {
			putRecord(table, values, data, offset)
			updateIndex(table, values, 0, (blockCnt-1)*table.Rib()+offset/size)
			buffer.BRelease(block)
			return
		}
	}
	// 说明没有空闲空间了，重新分配一个新页面
	buffer.BRelease(block)
	table.BlockCnt += 1
	block = buffer.BAddr(table.TableName, table.BlockCnt-1)
	data = block.Data
	putRecord(table, values, data, 0)
	updateIndex(table, values, 0, (table.BlockCnt-1)*table.Rib())
	buffer.BRelease(block)
}

func DeleteRecord(table *database.Table, conds []*database.Condition) {
	if table == nil {
		return
	}
	if conds == nil {
		conds = make([]*database.Condition, 0)
	}
	v := selectPos(table, conds)
	for _, pc := range v {
		block := buffer.BAddr(table.TableName, pc.first)
		if block == nil {
			continue
		}
		data := block.Data
		res := getRecord(table, data, pc.second)
		updateIndex(table, res, 1, 0)
		data[pc.second] = 0
		buffer.BRelease(block)
	}
}

func DeleteAllRecord(table *database.Table) {
	if table == nil {
		return
	}
	conds := make([]*database.Condition, 0)
	DeleteRecord(table, conds)
}

func SelectRecord(table *database.Table, conds []*database.Condition) [][]constant.Value {
	res := make([][]constant.Value, 0)
	if table == nil {
		return res
	}
	v := selectPos(table, conds)
	for _, pc := range v {
		block := buffer.BAddr(table.TableName, pc.first)
		if block == nil {
			continue
		}
		data := block.Data
		res = append(res, getRecord(table, data, pc.second))
		buffer.BRelease(block)
	}
	return res
}

func SelectAll(table *database.Table) [][]constant.Value {
	conds := make([]*database.Condition, 0)
	return SelectRecord(table, conds)
}

func checkType(col *database.Column, val constant.Value) bool {
	if col == nil {
		return false
	}
	var res bool = false
	switch col.Field {
	case constant.Int:
		_, res = constant.Int64Val(val)
	case constant.Float:
		_, res = constant.Float64Val(val)
	case constant.String:
		_ = constant.StringVal(val)
		res = true
	default:
		return false
	}
	return res
}

func checkUnique(table *database.Table, columnId int, val constant.Value) bool {
	if table == nil {
		return false
	}
	if table.Columns[columnId].Index != "" {
		cond := &database.Condition{
			ColumnName: table.Columns[columnId].ColumnName,
			Op:         token.EQL,
			Value:      val,
		}
		vec := selectByIndex(table, columnId, cond)
		return len(vec) <= 0
	}

	blockCnt := table.BlockCnt
	for i := 0; i < blockCnt; i++ {
		block := buffer.BAddr(table.TableName, i)
		if block == nil {
			continue
		}
		data := block.Data
		size := table.Size() + 1
		for offset := 0; offset < buffer.BlockSize; offset += size {
			if data[offset] == 1 {
				vals := getRecord(table, data, offset)
				if constant.Compare(vals[columnId], token.EQL, val) {
					buffer.BRelease(block)
					return false
				}
			}
		}
		buffer.BRelease(block)
	}
	return true
}

func getRecord(table *database.Table, data []byte, offset int) []constant.Value {
	res := make([]constant.Value, 0)
	if table == nil {
		return res
	}
	attrs := table.Columns
	size := len(attrs)
	offset += 1
	for i := 0; i < size; i++ {
		switch attrs[i].Field {
		case constant.Int:
			b := data[offset : offset+attrs[i].Size()]
			val := util.Byte2Int(b)
			res = append(res, constant.MakeInt64(int64(val)))
		case constant.Float:
			b := data[offset : offset+attrs[i].Size()]
			val := util.Byte2Float(b)
			res = append(res, constant.MakeFloat64(float64(val)))
		default:
			b := make([]byte, attrs[i].Size())
			copy(b, data[offset:offset+attrs[i].Size()])
			res = append(res, constant.MakeString(string(b)))
		}
		offset += attrs[i].Size()
	}
	return res
}

func putRecord(table *database.Table, vals []constant.Value, data []byte, offset int) {
	if table == nil || len(data) < offset {
		return
	}
	// 占位，表示这个地方已经有坑位了
	data[offset] = 1
	offset++
	attrs := table.Columns
	for i := 0; i < len(attrs); i++ {
		switch attrs[i].Field {
		case constant.Int:
			va, _ := constant.Int64Val(vals[i])
			vb := util.Int2Byte(int(va))
			copy(data[offset:], vb)
			offset += len(vb)
		case constant.Float:
			va, _ := constant.Float64Val(vals[i])
			vb := util.Float2Byte(float32(va))
			copy(data[offset:], vb)
			offset += len(vb)
		default:
			va := constant.StringVal(vals[i])
			vb := []byte(va)
			copy(data[offset:], vb)
			offset += attrs[i].CharSize
		}
	}
}

func updateIndex(table *database.Table, values []constant.Value, optype int, offset int) {
	if table == nil {
		return
	}
	cols := table.Columns
	for i := 0; i < len(values); i++ {
		if len(cols[i].Index) <= 0 {
			continue
		}
		switch optype {
		case 0:
			index.InsertIndex(table, cols[i].Index, cols[i].ColumnName, values[i], offset)
			break
		case 1:
			index.DeleteIndex(table, cols[i].Index, cols[i].ColumnName, values[i])
			break
		}
	}
}

func selectPos(table *database.Table, conds []*database.Condition) []piece {
	v := make([]piece, 0)
	if table == nil {
		return v
	}
	flag := false
	for _, cond := range conds {
		idx := table.IndexOfColumn(cond.ColumnName)
		if table.Columns[idx].Index != "" {
			// Not supported
			if cond.Op != token.EQL {
				break
			}
			if !flag {
				flag = true
				v = selectByIndex(table, idx, cond)
			} else {
				v = intersect(v, selectByIndex(table, table.IndexOfColumn(cond.ColumnName), cond))
			}
		}
	}
	// 从文件开始扫
	if !flag {
		blockCnt := table.BlockCnt
		for i := 0; i < blockCnt; i++ {
			block := buffer.BAddr(table.TableName, i)
			if block == nil {
				continue
			}
			data := block.Data
			size := table.Size() + 1
			for offset := 0; offset < buffer.BlockSize && offset+size < buffer.BlockSize; offset += size {
				if data[offset] == 0 {
					continue
				}
				res := getRecord(table, data, offset)
				good := true
				for _, cond := range conds {
					idx := table.IndexOfColumn(cond.ColumnName)
					if !cond.IsTrue(res[idx]) {
						good = false
						break
					}
				}
				if good {
					v = append(v, piece{
						first:  i,
						second: offset,
					})
				}
			}
			buffer.BRelease(block)
		}
	}
	return v
}

func selectByIndex(table *database.Table, columnId int, cond *database.Condition) []piece {
	res := make([]piece, 0)
	if table == nil || cond == nil {
		return res
	}
	attr := table.Columns[columnId]
	if attr.ColumnName != cond.ColumnName {
		fmt.Println("SelectByIndex error, column name inconsistent")
		return res
	}
	if cond.Op != token.EQL {
		fmt.Println("SelectByIndex error, range select not supported")
		return res
	}
	offset := index.FindIndex(table, attr.Index, attr.ColumnName, cond.Value)
	if offset < 0 {
		return res
	}
	rib := table.Rib()
	res = append(res, piece{
		first:  offset / rib,
		second: (offset % rib) * (table.Size() + 1),
	})
	return res
}

func intersect(p []piece, q []piece) []piece {
	sort.Slice(p, func(i, j int) bool {
		if p[i].first == p[j].first {
			return p[i].second < p[j].second
		}
		return p[i].first < p[j].first
	})
	sort.Slice(q, func(i, j int) bool {
		if q[i].first == q[j].first {
			return q[i].second < q[j].second
		}
		return q[i].first < q[j].first
	})
	res := make([]piece, 0, len(p)+len(q))
	for i, j := 0, 0; i < len(p) && j < len(q); {
		for i < len(p) && pieceCompare(p[i], q[j]) == -1 {
			i++
		}
		for j < len(q) && pieceCompare(q[j], p[i]) == -1 {
			j++
		}
		for i < len(p) && j < len(q) && pieceCompare(p[i], p[j]) == 0 {
			res = append(res, p[i])
			i++
			j++
		}
	}
	return res
}

func pieceCompare(p piece, q piece) int {
	if p.first < q.first {
		return -1
	}
	if p.first > q.first {
		return 1
	}
	return p.second - q.second
}
