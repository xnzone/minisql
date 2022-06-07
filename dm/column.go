package dm

import (
	"encoding/json"
)

type ColumnField int

const (
	ColumnFieldInt ColumnField = iota
	ColumnFieldFloat
	ColumnFieldChar
)

type Column struct {
	ColumnName   string      `json:"column_name"`
	Field        ColumnField `json:"field"`
	IsPrimaryKey bool        `json:"is_primary_key"`
	IsUnique     bool        `json:"is_unique"`
	CharSize     int         `json:"char_size"`
	Index        string      `json:"index"`
}

func TransStrColumn(str string) *Column {
	var col *Column
	_ = json.Unmarshal([]byte(str), &col)
	return col
}

func (b *Column) Size() int {
	switch b.Field {
	case ColumnFieldInt:
		return 4
	case ColumnFieldFloat:
		return 4
	case ColumnFieldChar:
		return b.CharSize + 1
	default:
		return b.CharSize + 1
	}
}

func (b *Column) String() string {
	bs, _ := json.Marshal(b)
	return string(bs)
}
