package interpreter

import (
	"fmt"
	"github.com/xnzone/minisql/api"
	"github.com/xnzone/minisql/database"
	"go/constant"
	"strings"
	"time"
)

func parseCreateTable(strvec []string) error {
	if len(strvec) < 7 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the primary key must be set when create table.")
		return fmt.Errorf("cmd is too short to create table")
	}
	tableName := strvec[2]
	_ = tableName
	var columns []*database.Column
	var primaryKey string
	var hasPrimaryKey bool = false
	var vecSize int = len(strvec)

	if strings.EqualFold(strvec[vecSize-7], "primary") && strings.EqualFold(strvec[vecSize-6], "key") {
		primaryKey = strvec[vecSize-4]
	} else {
		fmt.Println("ERROR: You have an error in your SQL syntax; the primary key must be set when create table.")
	}
	for i := 4; i < vecSize-7; {
		columnName := strvec[i]
		i++
		column := &database.Column{
			ColumnName: columnName,
		}
		i++
		if columnName == primaryKey {
			hasPrimaryKey = true
			column.IsPrimaryKey = true
			column.IsUnique = true
		}

		if strvec[i] == "int" {
			column.Field = constant.Int
			i++
		} else if strvec[i] == "float" {
			column.Field = constant.Float
			i++
		} else if strvec[i] == "char" {
			column.Field = constant.String
			i++
		} else {
			fmt.Printf("ERROR: You have an error in your SQL syntax; the type %s is not defined.\n", strvec[i])
			return fmt.Errorf("column type not defined")
		}
		if strvec[i] == "unique" {
			column.IsUnique = true
			i++
		}
		i++
		columns = append(columns, column)
	}
	if !hasPrimaryKey {
		fmt.Println("ERROR: You have an error in your SQL syntax; the primary key must be set when create table.")
		return fmt.Errorf("primary key nil")
	}
	start := time.Now().Unix()
	api.CreateTable(tableName, columns, primaryKey)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func parseDropTable(strvec []string) error {
	if len(strvec) < 3 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short when delete table.")
		return fmt.Errorf("cmd is too short to drop table")
	}
	tableName := strvec[2]
	_ = tableName
	start := time.Now().Unix()
	api.DropTable(tableName)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}
