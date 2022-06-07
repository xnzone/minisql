package cm

import (
	"fmt"
	"github.com/xnzone/minisql/dm"
	"testing"
)

func TestLoad(t *testing.T) {
	cm := &CatalogManager{
		tables: make(map[string]*dm.Table),
		Indices: make(map[string]*dm.Index),
	}
	cm.Load()
	fmt.Println(cm)
	for i := 0; i < 10; i++ {
		tableName := fmt.Sprintf("test_%d", i)
		cm.tables[tableName] = &dm.Table {
			TableName: tableName,
		}
		indexName := fmt.Sprintf("index_%d", i)
		cm.Indices[indexName] = &dm.Index{
			IndexName: indexName,
			TableName: tableName,
		}
	}
	t.Log(cm.tables)
	t.Log(cm.Indices)
	cm.Save()
}