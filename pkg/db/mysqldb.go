/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func NewMysqlDSN(dbUser, dbPassword, ipAddr, dbPort string, dbName string) (*Engine, error) {
	var (
		dsn string
	)
	if dbName != "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&timeout=120s&parseTime=true", dbUser, dbPassword, ipAddr,
			dbPort, dbName)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&timeout=120s&parseTime=true", dbUser, dbPassword, ipAddr,
			dbPort)
	}
	db, err := sql.Open("mysql", dsn) // this does not really open a new connection
	if err != nil {
		return &Engine{DB: db}, fmt.Errorf("error on initializing database connection: %v", err)
	}
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(10)

	err = db.Ping() // This DOES open a connection if necessary. This makes sure the database is accessible
	if err != nil {
		return &Engine{DB: db}, fmt.Errorf("error on opening database connection: %v", err)
	}

	return &Engine{DB: db}, nil
}

func (e *Engine) IsExistDbName(dbName string) bool {
	querySQL := fmt.Sprintf(`SELECT
	count( schema_name ) AS SCHEMA_NAME 
FROM
	information_schema.SCHEMATA 
WHERE
	lower( schema_name ) = lower( '%s' )`, dbName)
	_, res, _ := e.QuerySQL(querySQL)
	if res[0]["SCHEMA_NAME"] == "0" {
		return false
	}
	return true
}

// GetAllTables gets all table name from db
func GetAllTables(dbName string, engine *Engine) ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	table_name 
FROM
	information_schema.TABLES 
WHERE
	lower(table_schema) = lower('%s')`, dbName)

	var tables []string

	_, res, err := engine.QuerySQL(querySQL)
	if err != nil {
		return tables, fmt.Errorf("failed get all table name from db: %v", err)
	}
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}
