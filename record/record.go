package record

import (
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/index"
	"go/constant"
)

var (
	im *index.IndexManager
)

func Init() {
	im = index.NewIndexManager()
}

func CreateTable(table *database.Table) {

}

func DropTable(table *database.Table) {

}

func InsertRecord(table *database.Table, values []constant.Value) {

}

func DeleteRecord(table *database.Table, conds []*database.Condition) {

}

func DeleteAllRecord(table *database.Table) {

}

func SelectRecord(table *database.Table, conds []*database.Condition) [][]constant.Value {
	return nil
}

func SelectAll(table *database.Table) [][]constant.Value {
	return nil
}
