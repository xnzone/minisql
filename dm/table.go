package dm

import (
	"encoding/json"
	"github.com/xnzone/minisql/bm"
)

type Table struct {
	TableName     string         `json:"table_name"`
	Columns       []*Column      `json:"columns"`
	CMaps         map[string]int `json:"c_maps"`
	BlockCnt      int            `json:"block_cnt"`
	RecordSize    int            `json:"record_size"`
	RecordInBlock int            `json:"record_in_block"`
}

func TransStrTable(str string) *Table {
	var table *Table
	_ = json.Unmarshal([]byte(str), &table)
	return table
}

func (b *Table) HasColumn(columnName string) bool {
	for _, v := range b.Columns {
		if v == nil {
			continue
		}
		if v.ColumnName == columnName {
			return true
		}
	}
	return false
}

func (b *Table) GetColumn(columnName string) *Column {
	for k, v := range b.Columns {
		if v == nil {
			continue
		}
		if v.ColumnName == columnName {
			return b.Columns[k]
		}
	}
	return &Column{}
}

func (b *Table) IndexOfColumn(columnName string) int {
	if len(b.CMaps) <= 0 {
		for i := 0; i < len(b.Columns); i++ {
			b.CMaps[b.Columns[i].ColumnName] = i
		}
	}
	return b.CMaps[columnName]
}

func (b *Table) Size() int {
	if b.RecordSize > 0 {
		return b.RecordSize
	}
	for _, v := range b.Columns {
		if v == nil {
			continue
		}
		b.RecordSize += v.Size()
	}
	return b.RecordSize
}

func (b *Table) Rib() int {
	if b.RecordInBlock > 0 {
		return b.RecordInBlock
	}
	b.RecordInBlock = bm.BlockSize / (b.Size() + 1)
	return b.RecordInBlock
}

func (b *Table) String() string {
	sb, _ := json.Marshal(b)
	return string(sb)
}
