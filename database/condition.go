package database

import (
	"go/constant"
	"go/token"
)

type Condition struct {
	ColumnName string
	Op         token.Token
	Value      constant.Value
}

func IsCondTrue(lhs constant.Value, token token.Token, rhs constant.Value) bool {
	return constant.Compare(lhs, token, rhs)
}

func (b *Condition) IsTrue(value constant.Value) bool {
	return IsCondTrue(b.Value, b.Op, value)
}
