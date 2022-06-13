package interpreter

import (
	"fmt"
	"github.com/xnzone/minisql/api"
	"github.com/xnzone/minisql/database"
	"go/constant"
	"go/token"
	"strconv"
	"strings"
	"time"
)

func parseInsert(strvec []string) error {
	if len(strvec) < 4 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short")
		return fmt.Errorf("cmd is too short")
	}
	if strvec[1] != "into" || strvec[3] != "values" || strvec[4] != "(" {
		fmt.Println("ERROR: You have an error in your SQL syntax; you can use 'insert into TABLENAME values(VALUE1,VALUE2,...);' to insert.")
		return fmt.Errorf("cmd sql is not currect")
	}
	tableName := strvec[2]
	values := make([]constant.Value, 0)
	vecSize := len(strvec)
	if !api.ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax;table named %s doesn't exist", tableName)
		return fmt.Errorf("table not exist")
	}
	table := api.GetTable(tableName)
	for i, j := 5, 0; i < vecSize-1; i, j = i+2, j+1 {
		var v constant.Value
		column := table.Columns[j]
		var err error
		v, err = handleColumnField(column, strvec[i])
		if err != nil {
			return err
		}
		values = append(values, v)
	}
	start := time.Now().Unix()
	api.InsertOn(tableName, values)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func parseDelete(strvec []string) error {
	if len(strvec) < 3 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short")
		return fmt.Errorf("cmd is too short")
	}
	if strvec[1] != "from" {
		fmt.Println("ERROR: You have an error in your SQL syntax;you can use 'delete from TABLENAME;' or 'delete from TABLENAME where (CON1 and COND2 ..); to delete'")
		return fmt.Errorf("delete sql error")
	}
	tableName := strvec[2]
	conds := make([]*database.Condition, 0)
	vecSize := len(strvec)

	if strvec[3] == ";" {
		start := time.Now().Unix()
		api.DeleteAll(tableName)
		end := time.Now().Unix()
		fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
		return nil
	}

	if strvec[3] != "where" {
		fmt.Println("ERROR: You have an error in your SQL syntax;you can use 'delete from TABLENAME;' or 'delete from TABLENAME where (CON1 and COND2 ..); to delete'")
		return fmt.Errorf("delete sql error")
	}

	if !api.ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax;table named %s doesn't exist", tableName)
		return fmt.Errorf("table not exist")
	}

	table := api.GetTable(tableName)
	for i := 5; i < vecSize-1; {
		var cond *database.Condition
		var err error
		cond, i, err = handleColumnCond(table, strvec, i)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
		i += 2
	}
	start := time.Now().Unix()
	api.DeleteFrom(tableName, conds)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func parseSelect(strvec []string) error {
	if len(strvec) < 4 {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short")
		return fmt.Errorf("cmd is too short")
	}
	if strvec[1] != "*" || strvec[2] != "from" {
		fmt.Println("ERROR: You have an error in your SQL syntax;you can use 'select * from TABLENAME;' or 'select * from TABLENAME where (CON1 and COND2 ..); to select'")
		return fmt.Errorf("select sql error")
	}
	tableName := strvec[3]
	conds := make([]*database.Condition, 0)
	vecSize := len(strvec)

	if strvec[4] == ";" {
		start := time.Now().Unix()
		api.SelectAll(tableName)
		end := time.Now().Unix()
		fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
		return nil
	}

	if strvec[4] != "where" {
		fmt.Println("ERROR: You have an error in your SQL syntax;you can use 'select * from TABLENAME;' or 'select * from TABLENAME where (CON1 and COND2 ..); to select'")
		return fmt.Errorf("select sql error")
	}

	if !api.ExistTable(tableName) {
		fmt.Printf("ERROR: You have an error in your SQL syntax;table named %s doesn't exist", tableName)
		return fmt.Errorf("table not exist")
	}

	table := api.GetTable(tableName)
	for i := 6; i < vecSize-1; {
		var cond *database.Condition
		var err error
		cond, i, err = handleColumnCond(table, strvec, i)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
		i += 2
	}
	start := time.Now().Unix()
	api.Select(tableName, conds)
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func handleColumnCond(table *database.Table, strvec []string, i int) (*database.Condition, int, error) {
	columnName := strvec[i]
	i++
	var cond *database.Condition
	var op token.Token
	var err error
	op, i, err = handleColumnOp(strvec, i)
	if err != nil {
		return cond, i, err
	}
	i++

	var v constant.Value
	var j int
	for j = 0; j < len(table.Columns); j++ {
		if table.Columns[j].ColumnName == columnName {
			break
		}
	}
	if j == len(table.Columns) {
		fmt.Printf("ERROR: You have an error in your SQL syntax;column named %s doesn't exist", columnName)
		return cond, i, fmt.Errorf("column not exist")
	}
	column := table.Columns[j]
	v, err = handleColumnField(column, strvec[i])
	cond = &database.Condition{
		ColumnName: columnName,
		Op:         op,
		Value:      v,
	}
	return cond, i, nil
}

func handleColumnOp(strvec []string, i int) (token.Token, int, error) {
	var op token.Token
	if strvec[i] == "=" {
		op = token.EQL
	} else if strvec[i] == "<" && strvec[i+1] == ">" {
		op = token.NEQ
		i++
	} else if strvec[i] == "<" {
		if strvec[i+1] == ">" {
			op = token.NEQ
			i++
		} else if strvec[i+1] == "=" {
			op = token.LEQ
			i++
		} else {
			op = token.LSS
		}
	} else if strvec[i] == ">" {
		if strvec[i+1] == "=" {
			op = token.GEQ
			i++
		} else {
			op = token.GTR
		}
	} else {
		fmt.Printf("ERROR: You have an error in your SQL syntax;operator %s is not defined.", strvec[i])
		return op, i, fmt.Errorf("operator not exist")
	}
	return op, i, nil
}

func handleColumnField(column *database.Column, str string) (constant.Value, error) {
	var v constant.Value
	switch column.Field {
	case constant.Int:
		va, _ := strconv.ParseInt(str, 10, 64)
		v = constant.MakeInt64(va)
	case constant.Float:
		va, _ := strconv.ParseFloat(str, 10)
		v = constant.MakeFloat64(va)
	case constant.String:
		str = strings.Trim(str, "'")
		b := make([]byte, column.Size())
		copy(b, str)
		v = constant.MakeString(string(b))
	default:
		fmt.Println("ERROR: You have an error in your SQL syntax;column field not support")
		return v, fmt.Errorf("column not support")
	}
	return v, nil
}
