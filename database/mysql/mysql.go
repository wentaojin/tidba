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
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

type Database struct {
	DB *sql.DB
}

func NewDatabase(ctx context.Context, dsn string) (*Database, error) {
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error on open mysql database connection: %v", err)
	}

	// SetMaxIdleConns sets the maximum number of Databaseions in the idle Databaseion pool.
	sqlDB.SetMaxIdleConns(10)
	// SetMaxOpenConns sets the maximum number of open Databaseions to the database.
	sqlDB.SetMaxOpenConns(100)
	// SetConnMaxLifetime sets the maximum amount of time a Databaseion may be reused.
	sqlDB.SetConnMaxLifetime(time.Hour)

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("ping the sqlite database error: [%s]", err)
	}
	return &Database{DB: sqlDB}, nil
}

func BuildDatabaseDSN(username, password, host string, port uint64, charset string, connParams string) string {
	if !strings.EqualFold(charset, "") {
		connParams = fmt.Sprintf("charset=%s&%s", strings.ToLower(charset), connParams)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s", username, password, host, port, connParams)
}

func (d *Database) GetDatabase() interface{} {
	return d
}

func (d *Database) CloseDatabase() error {
	return d.DB.Close()
}

func (d *Database) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(ctx, query, args...)
}

func (d *Database) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DB.ExecContext(ctx, query, args...)
}

func (d *Database) GeneralQuery(ctx context.Context, query string, args ...any) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)

	rows, err := d.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query sql: [%v], error: %v", query, err)
	}
	defer rows.Close()

	// general query, automatic get column name
	columns, err = rows.Columns()
	if err != nil {
		return columns, results, fmt.Errorf("query rows.Columns failed, sql: [%v], error: %v", query, err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, results, fmt.Errorf("query rows.Scan failed, sql: [%v], error: %v", query, err)
		}

		row := make(map[string]string)
		for k, v := range values {
			if v == nil {
				row[columns[k]] = "NULLABLE"
			} else {
				// Handling empty string and other values, the return value output string
				row[columns[k]] = stringutil.BytesToString(v)
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return columns, results, fmt.Errorf("query rows.Next failed, sql: [%v], error: %v", query, err.Error())
	}
	return columns, results, nil
}

func (d *Database) QueryRows(ctx context.Context, query string, fn func(row, cols []string) error) (err error) {
	rows, err := d.QueryContext(ctx, query)
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
				result[i] = "'" + val + "'"
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

func (d *Database) GetRegionStatus(ctx context.Context, regionType string, regionArr []string, concurrency int) (*sync.Map, error) {
	var querySQL string

	regionsMap := &sync.Map{}

	regionSpitArr := stringutil.ArrayStringGroups(regionArr, 1000)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, r := range regionSpitArr {
		regions := r
		g.Go(func() error {
			if strings.EqualFold(regionType, "data") {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	TIDB_DECODE_KEY(START_KEY) AS START_KEY,
    TIDB_DECODE_KEY(END_KEY) AS END_KEY
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 0
    AND REGION_ID IN (%s)`, strings.Join(regions, ","))
			} else if strings.EqualFold(regionType, "index") {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	TIDB_DECODE_KEY(START_KEY) AS START_KEY,
    TIDB_DECODE_KEY(END_KEY) AS END_KEY
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 1
    AND REGION_ID IN (%s)`, strings.Join(regions, ","))
			} else {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	TIDB_DECODE_KEY(START_KEY) AS START_KEY,
    TIDB_DECODE_KEY(END_KEY) AS END_KEY
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    REGION_ID IN (%s)`, strings.Join(regions, ","))
			}
			_, res, err := d.GeneralQuery(gCtx, querySQL)
			if err != nil {
				return fmt.Errorf("failed get all store id from cluster: %v", err)
			}

			for _, s := range res {
				regionByte, err := json.Marshal(s)
				if err != nil {
					return err
				}

				if vals, ok := regionsMap.Load(s["REGION_ID"]); ok {
					var tmpRegionArr []string
					tmpRegionArr = append(tmpRegionArr, vals.([]string)...)
					tmpRegionArr = append(tmpRegionArr, string(regionByte))
					regionsMap.Store(s["REGION_ID"], tmpRegionArr)
				} else {
					regionsMap.Store(s["REGION_ID"], []string{string(regionByte)})
				}
			}
			return nil
		})

	}
	if err := g.Wait(); err != nil {
		return regionsMap, err
	}
	return regionsMap, nil
}
