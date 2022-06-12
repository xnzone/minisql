package catalog

import (
	"fmt"
	"github.com/xnzone/minisql/database"
	"github.com/xnzone/minisql/index"
	"github.com/xnzone/minisql/util"
	"io"
	"os"
)

const (
	catalogFile = "catalog.minisql"
)

type Catalog struct {
	indices map[string]*database.Index // index
	tables  map[string]*database.Table // table
}

var (
	cm *Catalog
)

func Init() {
	cm = &Catalog{
		indices: make(map[string]*database.Index),
		tables:  make(map[string]*database.Table),
	}
	cm.load()
	// 建立索引
	for _, idx := range cm.indices {
		table := GetTable(idx.TableName)
		index.CreateIndex(table, idx.IndexName, idx.ColumnName)
	}
}

func Flush() {
	cm.save()
}

func ExistTable(tableName string) bool {
	return cm.tables[tableName] != nil
}

func GetTable(tableName string) *database.Table {
	return cm.tables[tableName]
}

func NewTable(tableName string, columns []*database.Column) {
	table := &database.Table{
		TableName: tableName,
		Columns:   columns,
		CMaps:     make(map[string]int),
	}
	cm.tables[tableName] = table
}

func ExistIndex(indexName string) bool {
	return cm.indices[indexName] != nil
}

func NewIndex(indexName, tableName, columnName string) {
	cm.indices[indexName] = &database.Index{
		TableName:  tableName,
		IndexName:  indexName,
		ColumnName: columnName,
	}
	table := cm.tables[tableName]
	index := table.IndexOfColumn(columnName)
	column := table.Columns[index]
	column.Index = indexName
}

func DropTable(tableName string) {
	delete(cm.tables, tableName)
}

func GetIndex(indexName string) *database.Index {
	return cm.indices[indexName]
}

func DropIndex(indexName string) {
	index := cm.indices[indexName]
	defer delete(cm.indices, indexName)
	table := cm.tables[index.TableName]
	ioc := table.IndexOfColumn(index.ColumnName)
	table.Columns[ioc].Index = ""
}

func ValidName(name string) bool {
	return cm.tables[name] == nil && cm.indices[name] == nil
}

func (b *Catalog) load() {
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
			b.indices[string(isb)] = &database.Index{
				IndexName:  string(isb),
				TableName:  string(tsb),
				ColumnName: string(csb),
			}
		}
		_, err = fd.ReadAt(brd, offset)
	}
	return
}

func (b *Catalog) save() {
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
	for _, v := range b.indices {
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
