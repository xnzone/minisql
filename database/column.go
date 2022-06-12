package database

import (
	"encoding/json"
	"go/constant"
)

type Column struct {
	ColumnName   string        `json:"cn"`
	Field        constant.Kind `json:"field"`
	IsPrimaryKey bool          `json:"pkey"`
	IsUnique     bool          `json:"uniq"`
	CharSize     int           `json:"csz"`
	Index        string        `json:"idx"`
}

func (b *Column) TransByteColumn(sb []byte) {
	_ = json.Unmarshal(sb, &b)
}

func (b *Column) Size() int {
	switch b.Field {
	case constant.Int:
		return 4
	case constant.Float:
		return 4
	case constant.String:
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
