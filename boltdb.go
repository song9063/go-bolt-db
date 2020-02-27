package boltdb

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type BoltDBValue map[string]string
type BoltDBValueList []BoltDBValue


func DBMysqlConnect(host string, port string, userName string, pwd string, dbName string) (*sql.DB, error){
	var connectionString = "" + userName + ":" + pwd + "@tcp(" + host + ":" + port + ")/" + dbName
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}

	return db, err
}

// Insert
func DBMysqlInsert(dbConn *sql.Tx, strQuery string, args ...interface{}) (int64, error){
	var insertId int64 = -1
	res, mysqlErr := dbConn.Exec(strQuery, args...)
	if mysqlErr != nil {
		fmt.Println(mysqlErr)
		return -1, mysqlErr

	}else{
		insertId, mysqlErr = res.LastInsertId()
		if mysqlErr != nil {
			return -2, mysqlErr
		}
	}
	return insertId, nil
}

// Update or Delete
func DBMysqlExec(dbConn *sql.Tx, strQuery string, args ...interface{}) (int64, error){
	var rows int64 = -1
	res, mysqlErr := dbConn.Exec(strQuery, args...)
	if mysqlErr != nil {
		return -1, mysqlErr
	}else {
		rows, mysqlErr = res.RowsAffected()
		if mysqlErr != nil {
			return -2, mysqlErr
		}
	}

	return rows, nil
}

// Select
func DBMysqlSelect(dbConn *sql.DB, strQuery string, args ...interface{}) (BoltDBValueList, error){
	res, mysqlErr := dbConn.Query(strQuery, args...)
	if mysqlErr != nil {
		fmt.Println("\n=================")
		fmt.Println("Query Error!!")
		fmt.Println(mysqlErr)
		fmt.Println("=================\n")
		return nil, mysqlErr
	}

	cols, _ := res.Columns()
	aList := make([]BoltDBValue,0,50)
	arColumns := make([][]byte, len(cols))
	cPointers := make([]interface{}, len(cols))
	for i, _ := range arColumns {
		cPointers[i] = &arColumns[i]
	}

	for res.Next() {
		data := make(map[string]string)
		scanErr := res.Scan(cPointers...)
		if scanErr != nil {
			fmt.Println("scanError!!")
			fmt.Println(scanErr)
			return nil, scanErr
		}

		for i, colName := range cols {
			data[colName] = string(arColumns[i])
		}

		aList = append(aList, data)
	}

	return aList, nil
}
