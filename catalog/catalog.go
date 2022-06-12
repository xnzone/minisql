package catalog

import (
	"fmt"
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/util"
	"io"
	"os"
)

const (
	catalogFile = "catalog.minisql"
)

type CatalogManager struct {
	Indices map[string]*database.Index

	tables map[string]*database.Table
}

func Init() {

}

func ExistTable(tableName string) bool {
	return false
}

func GetTable(tableName string) *database.Table {
	return nil
}

func NewTable(tableName string, columns []*database.Column) {

}

func ExistIndex(indexName string) bool {
	return false
}

func NewIndex(indexName, tableName, columnName string) {

}

func DropTable(tableName string) {

}

func GetIndex(indexName string) *database.Index {
	return nil
}

func DropIndex(indexName string) {

}

func (b *CatalogManager) Load() {
	fd, err := os.OpenFile(catalogFile, os.O_RDONLY|os.O_CREATE, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("Open catalog file error: ", err.Error())
		os.Exit(10)
	}
	brd := make([]byte, 1, 1)
	var offset int64 = 0
	_, err = fd.ReadAt(brd, offset)
	for err != io.EOF {
		offset += 1
		if brd[0] == '1' {
			// 读表格
			var bt []byte
			bt, offset = loadByte(fd, offset)
			table := &database.Table{}
			table.TransByteTable(bt)
			b.tables[table.TableName] = table
		} else {
			// 读索引
			var isb, tsb, csb []byte
			isb, offset = loadByte(fd, offset)
			tsb, offset = loadByte(fd, offset)
			csb, offset = loadByte(fd, offset)
			b.Indices[string(isb)] = &database.Index{
				IndexName:  string(isb),
				TableName:  string(tsb),
				ColumnName: string(csb),
			}
		}
		_, err = fd.ReadAt(brd, offset)
	}
	return
}

func (b *CatalogManager) Save() {
	fd, err := os.OpenFile(catalogFile, os.O_RDWR, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("Open catalog file error: ", err.Error())
		os.Exit(10)
	}
	var offset int64 = 0
	for _, v := range b.tables {
		_, _ = fd.WriteAt([]byte{'1'}, offset)
		offset += 1
		bs := v.Bytes()
		ab := util.Int2Byte(len(bs))
		_, _ = fd.WriteAt(ab, offset)
		offset += 4
		_, _ = fd.WriteAt(bs, offset)
		offset += int64(len(bs))
	}
	// 如果有索引，写个0进去
	for _, v := range b.Indices {
		_, _ = fd.WriteAt([]byte{'0'}, offset)
		offset += 1

		isb := []byte(v.IndexName)
		isbl := util.Int2Byte(len(isb))
		_, _ = fd.WriteAt(isbl, offset)
		offset += int64(len(isbl))
		_, _ = fd.WriteAt(isb, offset)
		offset += int64(len(isb))

		tsb := []byte(v.TableName)
		tsbl := util.Int2Byte(len(tsb))
		_, _ = fd.WriteAt(tsbl, offset)
		offset += int64(len(tsbl))
		_, _ = fd.WriteAt(tsb, offset)
		offset += int64(len(tsb))

		csb := []byte(v.ColumnName)
		csbl := util.Int2Byte(len(csb))
		_, _ = fd.WriteAt(csbl, offset)
		offset += int64(len(csbl))
		_, _ = fd.WriteAt(csb, offset)
		offset += int64(len(csb))
	}
}

func (b *CatalogManager) CreateTable(tableName string, columns []*database.Column) {
	b.tables[tableName] = &database.Table{
		TableName: tableName,
		Columns:   columns,
	}
}

func (b *CatalogManager) CreateIndex(indexName string, tableName string, columnName string) {
	b.Indices[indexName] = &database.Index{
		TableName:  tableName,
		IndexName:  indexName,
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

func (b *CatalogManager) GetTable(tableName string) *database.Table {
	return b.tables[tableName]
}

func (b *CatalogManager) GetIndex(indexName string) *database.Index {
	return b.Indices[indexName]
}

func loadByte(fd *os.File, offset int64) ([]byte, int64) {
	bld := make([]byte, 4, 4)
	_, _ = fd.ReadAt(bld, offset)
	offset += int64(len(bld))
	bln := util.Byte2Int(bld)
	cld := make([]byte, bln, bln)
	_, _ = fd.ReadAt(cld, offset)
	offset += int64(len(cld))
	return cld, offset
}
