package interpreter

import (
	"fmt"
	"github.com/xnzone/minisql/api"
	"time"
)

func parseCreateIndex(strvec []string) error {
	if len(strvec) < 8 || strvec[3] != "on" || strvec[5] != "(" || strvec[7] != ")" {
		fmt.Println("ERROR: You have an error in your SQL syntax; you can use 'create index INDEXNAME on TABLENAME(COLUMNNAME);' to create index.")
		return fmt.Errorf("syntax error")
	}
	indexName := strvec[2]
	tableName := strvec[4]
	columnName := strvec[6]
	_, _, _ = indexName, tableName, columnName
	start := time.Now().Unix()
	api.CreateIndex(indexName, tableName, columnName)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func parseDropIndex(strvec []string) error {
	if len(strvec) < 3 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short")
		return fmt.Errorf("cmd is too short to drop index")
	}
	indexName := strvec[2]
	_ = indexName
	start := time.Now().Unix()
	api.DropIndex(indexName)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}
