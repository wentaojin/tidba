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
)

type Engine struct {
	MySQLDB *sql.DB
}

func (e *Engine) Query(querySQL string) (cols []string, res []map[string]string, err error) {
	return Query(e.MySQLDB, querySQL)
}

// query returns table field columns and corresponding field row data
func Query(db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.Query(querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("error on general query sql \n%v \nFailed: %v", querySQL, err.Error())
	}
	defer rows.Close()

	// indefinite field query,get field names automatically
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("error on query rows.Columns failed: %v", err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("error on query rows.Scan failed: %v", err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}
		res = append(res, row)
	}
	return cols, res, nil
}
