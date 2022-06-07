package cm

import (
	"fmt"
	"github.com/xnzone/minisql/dm"
	"os"
)

const (
	catalogFile = "catalog.minisql"
)

type CatalogManager struct {
	Indices map[string]*dm.Index

	tables map[string]*dm.Table
}

func Load() *CatalogManager {
	fd, err := os.OpenFile(catalogFile, os.O_RDONLY | os.O_CREATE, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("Open catalog file error", err.Error())
		os.Exit(10)
	}
	return nil
}

func (b *CatalogManager) CreateTable(tableName string, columns []*dm.Column) {
	b.tables[tableName] = &dm.Table{
		TableName: tableName,
		Columns: columns,
	}
}

func (b *CatalogManager) CreateIndex(indexName string, tableName string, columnName string) {
	b.Indices[indexName] = &dm.Index{
		TableName: tableName,
		IndexName: indexName,
		ColumnName: columnName,
	}
	table := b.tables[tableName]
	index := table.IndexOfColumn(columnName)
	column := table.Columns[index]
	column.Index = indexName
}

func (b *CatalogManager) DropTable(tableName string) {
	delete(b.tables, tableName)
}

func (b *CatalogManager) DropIndex(indexName string) {
	index := b.Indices[indexName]
	defer delete(b.Indices, indexName)
	table := b.tables[index.TableName]
	ioc := table.IndexOfColumn(index.ColumnName)
	table.Columns[ioc].Index = ""
}

func (b *CatalogManager) ExistTable(tableName string) bool {
	return b.tables[tableName] != nil
}

func (b *CatalogManager) ExistIndex(indexName string) bool {
	return b.Indices[indexName] != nil
}

func (b *CatalogManager) ValidName(name string) bool {
	return b.tables[name] == nil && b.Indices[name] == nil
}

func (b *CatalogManager) GetTable(tableName string) *dm.Table {
	return b.tables[tableName]
}

func (b *CatalogManager) GetIndex(indexName string) *dm.Index {
	return b.Indices[indexName]
}
