package catalog

import (
	"fmt"
	"github.com/xnzone/minisql/database"
	"testing"
)

func TestLoad(t *testing.T) {
	cm := &CatalogManager{
		tables:  make(map[string]*database.Table),
		Indices: make(map[string]*database.Index),
	}
	cm.Load()
	fmt.Println(cm)
	for i := 0; i < 10; i++ {
		tableName := fmt.Sprintf("test_%d", i)
		cm.tables[tableName] = &database.Table{
			TableName: tableName,
		}
		indexName := fmt.Sprintf("index_%d", i)
		cm.Indices[indexName] = &database.Index{
			IndexName: indexName,
			TableName: tableName,
		}
	}
	t.Log(cm.tables)
	t.Log(cm.Indices)
	cm.Save()
}
