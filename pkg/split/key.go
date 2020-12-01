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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/WentaoJin/tidba/zlog"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/util"
)

func IncludeTableSplitKey(dbName string, statusAddr string, concurrency int, includeTables []string, outDir string, engine *db.Engine) error {
	var tasks []*Task

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db [%s] table '%v' not exist", dbName, notExistTables)
	}

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}

func FilterTableSplitKey(dbName string, statusAddr string, concurrency int, excludeTables []string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAllTables(allTables, excludeTables)

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)
	return nil

}

func RegexpTableSplitKey(dbName string, statusAddr string, concurrency int, regex string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	includeTables := util.RegexpFromAllTables(allTables, regex)

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}

func AllTableSplitKey(dbName string, statusAddr string, concurrency int, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}
	var tasks []*Task
	for i, table := range allTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}

/*
	Split key
*/
// split table region by key
func taskConcurrencyRunKey(tasks []*Task, concurrency int, fn func(engine *db.Engine, dbName, tableName, outDir string, statusAddr string, loc *time.Location) (TableInfo, error)) {
	wg := sync.WaitGroup{}
	jobsChannel := make(chan *Task, len(tasks))
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				tableInfo, err := fn(j.Engine, j.DbName, j.TableName, j.OutDir, j.StatusAddr, j.Loc)
				if err != nil {
					zlog.Logger.Fatal("Task execute failed",
						zap.Int("taskID", j.TaskID),
						zap.Int("threadID", threadID),
						zap.Error(err),
					)
				}
				t, err := json.Marshal(tableInfo)
				if err != nil {
					zlog.Logger.Fatal("Task json table info struct failed",
						zap.Int("taskID", j.TaskID),
						zap.Int("threadID", threadID),
						zap.String("tableInfo", string(t)),
					)
				}
				zlog.Logger.Info("Task execute success",
					zap.Int("taskID", j.TaskID),
					zap.Int("threadID", threadID),
					zap.String("tableInfo", string(t)))
				wg.Done()
			}
		}(i)
	}

	for _, task := range tasks {
		jobsChannel <- task
		wg.Add(1)

	}
	wg.Wait()
}

func runSplitTableKey(engine *db.Engine, dbName string, tableName string, outDir string, statusAddr string, loc *time.Location) (TableInfo, error) {
	var (
		tableInfo TableInfo
		err       error
	)
	startTime := time.Now()
	tableInfo, err = splitKeyTableRun(engine, dbName, tableName, outDir, statusAddr, loc)
	endTime := time.Now()
	if err != nil {
		tableInfo.CostTime = endTime.Sub(startTime).String()
		return tableInfo, err
	} else {
		tableInfo.CostTime = endTime.Sub(startTime).String()
	}
	return tableInfo, nil
}

func splitKeyTableRun(engine *db.Engine, dbName string, tableName string, outDir string, statusAddr string, loc *time.Location) (TableInfo, error) {
	var tableInfo TableInfo
	tableInfo.DbName = dbName
	tableInfo.TableName = tableName
	tableInfo.OutDir = outDir

	// get table info
	// get table all data key
	startTime := time.Now()
	query := fmt.Sprintf(`SELECT
	START_KEY 
FROM
	information_schema.tikv_region_status 
WHERE
	lower(db_name) = lower('%s') 
	AND lower(table_name) = lower('%s') 
	AND is_index = 0 
ORDER BY
	start_key`, dbName, tableName)

	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})

	var colValues []string
	if err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			return fmt.Errorf("table [%s.%s] is not reocrd, should never happen", dbName, tableName)
		}
		colValues = append(colValues, row[0])
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}

	if len(colValues) <= 10 {
		return tableInfo, fmt.Errorf("table [%s.%s] region nums is lower than 10, it's needn't split", dbName, tableName)
	}

	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate table data region key total time", endTime.Sub(startTime).String()),
	)

	startTime = time.Now()
	var dataDecodeKeys []string
	// exclude max region key, because max key can't decode
	for _, col := range colValues[1:] {
		key, err := decodeKeyFromString(col, statusAddr, loc)
		if err != nil {
			return tableInfo, err
		}
		dataDecodeKeys = append(dataDecodeKeys, key)
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("decode all table data region key total time", endTime.Sub(startTime).String()),
	)

	// init dir and file wirter
	if err := initOrCreateDir(tableInfo.OutDir); err != nil {
		return tableInfo, err
	}

	outFile, err := os.OpenFile(fmt.Sprintf("%s/split_key_%s.SQL", tableInfo.OutDir, tableInfo.TableName), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return tableInfo, fmt.Errorf("open file [%s] failed: %v", fmt.Sprintf("%s/split_key_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}
	var (
		fileWriter *bufio.Writer
		file       *os.File
	)
	fileWriter, file = bufio.NewWriter(outFile), outFile

	// generate split data sql file
	startTime = time.Now()
	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` by ", tableInfo.DbName, tableInfo.TableName))
	for _, key := range dataDecodeKeys {
		sqlBuf.WriteString("('")
		keys := strings.Split(gjson.Get(key, "_tidb_rowid").String(), ",")
		if len(keys) == 1 {
			sqlBuf.WriteString(keys[0])
		} else {
			sqlBuf.WriteString(strings.Join(keys, "','"))
		}
		sqlBuf.WriteString("')")
		sqlBuf.WriteString(",")
	}
	_, err = fileWriter.WriteString(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n")
	if err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate all table data split region sql total time", endTime.Sub(startTime).String()),
	)

	// get table all index names
	startTime = time.Now()
	query = fmt.Sprintf(`SELECT DISTINCT
	KEY_NAME
	FROM
	INFORMATION_SCHEMA.TIDB_INDEXES
	WHERE
	lower( table_name ) = lower( '%s' )
	AND lower( table_schema ) = lower( '%s' )`, tableName, dbName)

	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})

	if err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			return fmt.Errorf("table [%s.%s] is not index name, should never happen", dbName, tableName)
		}
		tableInfo.Indexes = append(tableInfo.Indexes, IndexInfo{IndexName: row[0]})
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}

	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get table all index name total time", endTime.Sub(startTime).String()),
	)

	// get table index all columns value
	startTime = time.Now()
	for i := range tableInfo.Indexes {
		query = fmt.Sprintf(`SELECT
	COLUMN_NAME 
FROM
	INFORMATION_SCHEMA.TIDB_INDEXES 
WHERE
	lower( table_name ) = lower( '%s' ) 
	AND lower( table_schema ) = lower( '%s' ) 
	AND key_name = '%s' 
ORDER BY
	SEQ_IN_INDEX`, tableName, dbName, tableInfo.Indexes[i].IndexName)

		tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
			SQL: query,
		})

		var columns []string
		if err := queryRows(engine.DB, query, func(row, cols []string) error {
			if len(row) != 1 {
				return fmt.Errorf("table [%s.%s] is not index column name, should never happen", dbName, tableName)
			}
			columns = append(columns, row[0])
			return nil
		}); err != nil {
			return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}
		tableInfo.Indexes[i].ColumnName = columns
	}

	// find _tidb_rowid handle name(bigint or tidb_rowid)
	query = fmt.Sprintf("select * from %s.%s where _tidb_rowid = 1", dbName, tableName)
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})
	err = queryRows(engine.DB, query, func(row, cols []string) error {
		return nil
	})
	if err == nil {
		tableInfo.HandleName = "_tidb_rowid"
	} else {
		// handle primary key is bigint
		for i, idx := range tableInfo.Indexes {
			if len(idx.ColumnName) == 1 && strings.ToLower(idx.IndexName) == "primary" {
				tableInfo.HandleName = idx.ColumnName[0]
				Indexes := tableInfo.Indexes[:i]
				Indexes = append(Indexes, tableInfo.Indexes[i+1:]...)
				tableInfo.Indexes = Indexes
			}
		}
	}

	if tableInfo.HandleName == "" {
		return tableInfo, fmt.Errorf("can not found the handle column of table %s.%s", dbName, tableName)
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get table all index column total time", endTime.Sub(startTime).String()),
	)

	// get index region key
	startTime = time.Now()
	for _, idx := range tableInfo.Indexes {
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", tableInfo.DbName, tableInfo.TableName, idx.IndexName))

		query = fmt.Sprintf(`SELECT
	START_KEY 
FROM
	information_schema.tikv_region_status 
WHERE
	lower(db_name) = lower('%s') 
	AND lower(table_name) = lower('%s') 
	AND lower(index_name) = lower('%s') 
	AND is_index = 1
ORDER BY
	start_key`, dbName, tableName, idx.IndexName)
		tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
			SQL: query,
		})

		var colIndexValues []string
		if err = queryRows(engine.DB, query, func(row, cols []string) error {
			if len(row) != 1 {
				return fmt.Errorf("table [%s.%s] is not index column name, should never happen", dbName, tableName)
			}
			colIndexValues = append(colIndexValues, row[0])
			return nil
		}); err != nil {
			return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}

		var indexDecodeKeys []string
		// exclude max region key, because max key can't decode
		for _, col := range colIndexValues[1:] {
			key, err := decodeKeyFromString(col, statusAddr, loc)
			if err != nil {
				return tableInfo, err
			}
			keys := gjson.Get(key, "index_vals").String()
			indexDecodeKeys = append(indexDecodeKeys, keys)
		}

		// generate split region key sql file
		for _, indexKey := range indexDecodeKeys {
			if len(idx.ColumnName) == 1 {
				key := gjson.Get(indexKey, idx.ColumnName[0]).String()
				sqlBuf.WriteString("('")
				sqlBuf.WriteString(strings.TrimSpace(key))
				sqlBuf.WriteString("')")
				sqlBuf.WriteString(",")
			} else {
				sqlBuf.WriteString("(")
				var colKeys []string
				for _, col := range idx.ColumnName {
					key := gjson.Get(indexKey, col).String()
					colKeys = append(colKeys, fmt.Sprintf(`'%s'`, strings.TrimSpace(key)))
				}
				sqlBuf.WriteString(strings.Join(colKeys, ","))
				sqlBuf.WriteString(")")
				sqlBuf.WriteString(",")
			}
		}

		_, err = fileWriter.WriteString(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n")
		if err != nil {
			return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate table all index split region sql total time", endTime.Sub(startTime).String()),
	)

	// SQL output flush to file
	if err := fileWriter.Flush(); err != nil {
		return tableInfo, fmt.Errorf("split range region SQL wirte into [%v] failed: %v", fmt.Sprintf("%s/split_key_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}
	if err := file.Close(); err != nil {
		return tableInfo, fmt.Errorf("os file [%v] close failed: %v", fmt.Sprintf("%s/split_key_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}

	return tableInfo, nil
}
