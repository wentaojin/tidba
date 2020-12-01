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
package split

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/WentaoJin/tidba/pkg/db"

	"github.com/WentaoJin/tidba/pkg/util"
)

// Split table info
type TableInfo struct {
	DbName     string
	TableName  string
	Indexes    []IndexInfo
	HandleName string
	SQL        []SqlInfo
	OutDir     string
	CostTime   string
}

type SqlInfo struct {
	SQL string
}

type IndexInfo struct {
	IndexName  string
	ColumnName []string
}

// Split task
type Task struct {
	TaskID     int
	DbName     string
	TableName  string
	OutDir     string
	Engine     *db.Engine
	Loc        *time.Location
	StatusAddr string
}

// initOrCreateDir
func initOrCreateDir(path string) error {
	exist, err := util.IsExistPath(path)
	if err != nil {
		return fmt.Errorf("get dir [%s] failed: %v", path, err)
	}
	if !exist {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("mkdir dir [%s] failed: %v", path, err)
		}
	}

	return nil
}

/*
	Common db query
*/
func queryRows(Engine *sql.DB, SQL string, fn func(row, cols []string) error) (err error) {
	rows, err := Engine.Query(SQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return err
	}

	cols, err1 := rows.Columns()
	if err1 != nil {
		return err1
	}
	// Read all rows.
	var actualRows [][]string
	for rows.Next() {

		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return err1
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for _, row := range actualRows {
		err := fn(row, cols)
		if err != nil {
			return err
		}
	}
	return nil
}
