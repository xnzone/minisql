package database

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
	ColumnName   string      `json:"cn"`
	Field        ColumnField `json:"field"`
	IsPrimaryKey bool        `json:"pkey"`
	IsUnique     bool        `json:"uniq"`
	CharSize     int         `json:"csz"`
	Index        string      `json:"idx"`
}

func TransByteColumn(sb []byte) *Column {
	var col *Column
	_ = json.Unmarshal(sb, &col)
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
	return string(b.Bytes())
}

func (b *Column) Bytes() []byte {
	bs, _ := json.Marshal(b)
	return bs
}
