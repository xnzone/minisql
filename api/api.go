package api

import (
	"fmt"
	"github.com/xnzone/minisql/buffer"
	"github.com/xnzone/minisql/catalog"
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/index"
	"github.com/xnzone/minisql/record"
	"go/constant"
)

func Init() {
	buffer.Init()  // buffer最先初始化
	catalog.Init() // 初始化目录
	index.Init()   // 初始化索引
	record.Init()  // 初始化记录
}

func Flush() {
	catalog.Flush()
}

func CreateTable(tableName string, columns []*database.Column, primaryKey string) {
	if ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s already exist.\n", tableName)
		return
	}
	// 创建table
	catalog.NewTable(tableName, columns)
	table := GetTable(tableName)
	if table == nil {
		return
	}
	// 给记录创建table
	record.CreateTable(table)
	// 创建索引
	CreateIndex(primaryIndexName(tableName), tableName, primaryKey)
}

func CreateIndex(indexName string, tableName string, columnName string) {
	if catalog.ExistIndex(indexName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; index named %s already exist.\n", indexName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	// 目录创建索引信息
	catalog.NewIndex(indexName, tableName, columnName)
	// 创建索引
	index.CreateIndex(table, indexName, columnName)
}

func DropTable(tableName string) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	record.DropTable(table)
	catalog.DropTable(tableName)
	DropIndex(primaryIndexName(tableName))
}

func DropIndex(indexName string) {
	if !catalog.ExistIndex(indexName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; index named %s doesn't exist.\n", indexName)
		return
	}
	idx := catalog.GetIndex(indexName)
	if idx == nil {
		return
	}
	table := GetTable(idx.TableName)
	if table == nil {
		return
	}
	// 删除目录索引
	catalog.DropIndex(indexName)
	// 删除索引
	index.DropIndex(table, indexName, idx.ColumnName)
}

func InsertOn(tableName string, values []constant.Value) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	valNum, colNum := len(values), len(table.Columns)
	if valNum != colNum {
		fmt.Printf("ERROR: You have an error in you SQL syntax; table named %s has %d columns but %d values are given.\n", tableName, colNum, valNum)
		return
	}
	record.InsertRecord(table, values)

}

func DeleteFrom(tableName string, conds []*database.Condition) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	for _, cond := range conds {
		if !table.HasColumn(cond.ColumnName) {
			fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't have a column named %s.\n", tableName, cond.ColumnName)
			return
		}
	}
	record.DeleteRecord(table, conds)
}

func DeleteAll(tableName string) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	record.DeleteAllRecord(table)
}

func Select(tableName string, conds []*database.Condition) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	for _, cond := range conds {
		if !table.HasColumn(cond.ColumnName) {
			fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't have a column named %s.\n", tableName, cond.ColumnName)
			return
		}
	}
	result := record.SelectRecord(table, conds)
	printRecord(table.Columns, result)
}

func SelectAll(tableName string) {
	if !ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax; table named %s doesn't exist.\n", tableName)
		return
	}
	table := GetTable(tableName)
	if table == nil {
		return
	}
	result := record.SelectAll(table)
	printRecord(table.Columns, result)
}

func ExistTable(tableName string) bool {
	return catalog.ExistTable(tableName)
}

func GetTable(tableName string) *database.Table {
	return catalog.GetTable(tableName)
}

func primaryIndexName(tableName string) string {
	return fmt.Sprintf("%s_Primary", tableName)
}

func printRecord(columns []*database.Column, result [][]constant.Value) {
	if result == nil || columns == nil {
		return
	}
	for _, column := range columns {
		fmt.Print(column.ColumnName)
	}
	fmt.Println()
	for _, res := range result {
		for i, col := range res {
			switch columns[i].Field {
			case constant.Int:
				fmt.Print(constant.Int64Val(col))
			case constant.Float:
				fmt.Print(constant.Float64Val(col))
			case constant.String:
				fmt.Print(constant.StringVal(col))
			default:
				fmt.Print(constant.StringVal(col))
			}
		}
		fmt.Println()
	}
	fmt.Printf("%d rows in set\n", len(result))
}
