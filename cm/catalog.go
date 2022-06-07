package cm

import (
	"fmt"
	"github.com/xnzone/minisql/com"
	"github.com/xnzone/minisql/dm"
	"io"
	"os"
)

const (
	catalogFile = "catalog.minisql"
)

type CatalogManager struct {
	Indices map[string]*dm.Index

	tables map[string]*dm.Table
}

func (b *CatalogManager) Load() {
	fd, err := os.OpenFile(catalogFile, os.O_RDONLY | os.O_CREATE, 0666)
	defer func() { _ = fd.Close() }()
	if err != nil {
		fmt.Println("Open catalog file error: ", err.Error())
		os.Exit(10)
	}
	brd := make([]byte, 1, 1)
	var offset int64 = 0
	for _, err = fd.ReadAt(brd, offset); err != io.EOF; {
		offset += 1
		if brd[0] == 1 {
			// 读表格
			var bt []byte
			bt, offset = loadByte(fd, offset)
			table := dm.TransByteTable(bt)
			if table == nil {
				continue
			}
			b.tables[table.TableName] = table
		} else {
			// 读索引
			var isb, tsb, csb []byte
			isb, offset = loadByte(fd, offset)
			tsb, offset = loadByte(fd, offset)
			csb, offset = loadByte(fd, offset)
			b.Indices[string(isb)] = &dm.Index{
				IndexName: string(isb),
				TableName: string(tsb),
				ColumnName: string(csb),
			}
		}
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
	for _, v := range b.tables {
		_, _ = fd.Write([]byte{0})
		bs := v.Bytes()
		a := len(bs)
		_, _ = fd.Write(com.Int2Byte(a))
		_, _ = fd.Write(bs)
	}
	// 如果有索引，写个0进去
	for _, v := range b.Indices {
		_, _ = fd.Write([]byte{0})

		isb := []byte(v.IndexName)
		_, _ = fd.Write(com.Int2Byte(len(isb)))
		_, _ = fd.Write(isb)

		tsb := []byte(v.TableName)
		_, _ = fd.Write(com.Int2Byte(len(tsb)))
		_, _ = fd.Write(tsb)

		csb := []byte(v.ColumnName)
		_, _ = fd.Write(com.Int2Byte(len(csb)))
		_, _ = fd.Write(csb)
	}
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

func loadByte(fd *os.File, offset int64) ([]byte, int64) {
	bld := make([]byte, 4, 4)
	_, _ = fd.ReadAt(bld, offset)
	offset += int64(len(bld))
	bln := com.Byte2Int(bld)
	cld := make([]byte, bln, bln)
	_, _ = fd.Read(cld)
	offset += int64(len(cld))
	return cld, offset
}