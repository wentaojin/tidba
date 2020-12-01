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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"

	"github.com/WentaoJin/tidba/pkg/util"
)

func IncludeTableSplitRange(dbName string, concurrency int, includeTables []string, outDir string, engine *db.Engine) error {
	var tasks []*Task

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db [%s] table '%v' not exist", dbName, notExistTables)
	}

	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:    i,
			DbName:    dbName,
			TableName: table,
			OutDir:    outDir,
			Engine:    engine,
		})
	}

	taskConcurrencyRunRange(tasks, concurrency, runSplitTableRange)

	return nil
}

func FilterTableSplitRange(dbName string, concurrency int, excludeTables []string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAllTables(allTables, excludeTables)

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:    i,
			DbName:    dbName,
			TableName: table,
			OutDir:    outDir,
			Engine:    engine,
		})
	}

	taskConcurrencyRunRange(tasks, concurrency, runSplitTableRange)

	return nil

}

func RegexpTableSplitRange(dbName string, concurrency int, regex string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	includeTables := util.RegexpFromAllTables(allTables, regex)

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:    i,
			DbName:    dbName,
			TableName: table,
			OutDir:    outDir,
			Engine:    engine,
		})
	}

	taskConcurrencyRunRange(tasks, concurrency, runSplitTableRange)

	return nil
}

func AllTableSplitRange(dbName string, concurrency int, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	var tasks []*Task
	for i, table := range allTables {
		tasks = append(tasks, &Task{
			TaskID:    i,
			DbName:    dbName,
			TableName: table,
			OutDir:    outDir,
			Engine:    engine,
		})
	}

	taskConcurrencyRunRange(tasks, concurrency, runSplitTableRange)
	return nil
}

/*
	Split range
*/
// split table region by range
func taskConcurrencyRunRange(tasks []*Task, concurrency int, fn func(engine *db.Engine, dbName, tableName, outDir string) (TableInfo, error)) {
	wg := sync.WaitGroup{}
	jobsChannel := make(chan *Task, len(tasks))
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				tableInfo, err := fn(j.Engine, j.DbName, j.TableName, j.OutDir)
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

func runSplitTableRange(engine *db.Engine, dbName string, tableName string, outDir string) (TableInfo, error) {
	var (
		tableInfo TableInfo
		err       error
	)
	startTime := time.Now()
	tableInfo, err = splitRangeTableRun(engine, dbName, tableName, outDir)
	endTime := time.Now()
	if err != nil {
		tableInfo.CostTime = endTime.Sub(startTime).String()
		return tableInfo, err
	} else {
		tableInfo.CostTime = endTime.Sub(startTime).String()
	}
	return tableInfo, nil
}

// splitRangeTableRun is used to generate split table range
func splitRangeTableRun(engine *db.Engine, dbName string, tableName string, outDir string) (TableInfo, error) {
	var tableInfo TableInfo
	tableInfo.DbName = dbName
	tableInfo.TableName = tableName
	tableInfo.OutDir = outDir

	// get table info
	// get table all index names
	startTime := time.Now()
	query := fmt.Sprintf(`SELECT DISTINCT
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

	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("sql get table all index name total time", endTime.Sub(startTime).String()),
	)

	startTime = time.Now()
	// get table index all columns value
	for i := range tableInfo.Indexes {
		query = fmt.Sprintf(`SELECT
	COLUMN_NAME 
FROM
	INFORMATION_SCHEMA.TIDB_INDEXES 
WHERE
	lower( table_name ) = lower( '%s' ) 
	AND lower( table_schema ) = lower( '%s' ) 
	AND lower( key_name ) = lower( '%s' ) 
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
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("sql get table all index column total time", endTime.Sub(startTime).String()),
	)

	// find _tidb_rowid handle name(bigint or tidb_rowid)
	query = fmt.Sprintf("select * from %s.%s where _tidb_rowid = 1", dbName, tableName)
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})
	err := queryRows(engine.DB, query, func(row, cols []string) error {
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

	tableInfo, err = splitRangeTableOutputFile(engine, tableInfo)
	if err != nil {
		return tableInfo, err
	}

	return tableInfo, nil
}

func splitRangeTableOutputFile(engine *db.Engine, tableInfo TableInfo) (TableInfo, error) {
	if err := initOrCreateDir(tableInfo.OutDir); err != nil {
		return tableInfo, err
	}

	outFile, err := os.OpenFile(fmt.Sprintf("%s/split_range_%s.SQL", tableInfo.OutDir, tableInfo.TableName), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return tableInfo, fmt.Errorf("open file [%s] failed: %v", fmt.Sprintf("%s/split_range_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}
	var (
		fileWriter *bufio.Writer
		file       *os.File
	)
	fileWriter, file = bufio.NewWriter(outFile), outFile

	// get table total count
	startTime := time.Now()
	totalRowCount := 0
	query := fmt.Sprintf("select count(*) from %s.%s", tableInfo.DbName, tableInfo.TableName)
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})

	if err = queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			return fmt.Errorf("result row is not index column name, should never happen")
		}
		v, err := strconv.Atoi(row[0])
		if err != nil {
			return err
		}
		totalRowCount = v
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}
	if totalRowCount == 0 {
		return tableInfo, fmt.Errorf("table [%s.%s] row count is 0, can not probe region range", tableInfo.DbName, tableInfo.TableName)
	}

	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("sql get table count total time", endTime.Sub(startTime).String()),
	)

	_, err = engine.DB.Exec("use " + tableInfo.DbName)
	if err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", "use"+tableInfo.DbName, err)
	}

	// 	generate table index region range SQL
	startTime = time.Now()
	indexTotalRegionCount := 0
	for _, idx := range tableInfo.Indexes {
		// get index regions(show table regions)
		query := fmt.Sprintf("show table `%s`.`%s` index `%s` regions", tableInfo.DbName, tableInfo.TableName, idx.IndexName)
		tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
			SQL: query,
		})
		count := 0
		err := queryRows(engine.DB, query, func(row, cols []string) error {
			count++
			return nil
		})
		if err != nil {
			return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}
		indexTotalRegionCount += count
		if count == 0 {
			return tableInfo, fmt.Errorf("generate table index region range count value equal to 0，integer divide by zero")
		}

		step := totalRowCount / count
		if step == 0 {
			zlog.Logger.Warn("Split region", zap.String("warn", fmt.Sprintf("index [%s] of table [%s.%s] step is 0, can not probe index region range", idx.IndexName, tableInfo.DbName, tableInfo.TableName)))
			continue
		}

		// rownum get split region value
		query = fmt.Sprintf("select `%[1]s` from (select row_number() over (order by `%[1]s`) as row_num,`%[1]s` from `%[2]s`.`%[3]s` order by `%[1]s`) as t where t.row_num %% %[4]d = 0", strings.Join(idx.ColumnName, "`,`"), tableInfo.DbName, tableInfo.TableName, step)
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", tableInfo.DbName, tableInfo.TableName, idx.IndexName))

		cnt := 0
		tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
			SQL: query,
		})
		if err = queryRows(engine.DB, query, func(row, cols []string) error {
			if cnt > 0 {
				sqlBuf.WriteString(",")
			}
			cnt++
			sqlBuf.WriteString("('")
			sqlBuf.WriteString(strings.Join(row, "','"))
			sqlBuf.WriteString("')")
			return nil
		}); err != nil {
			return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}
		_, err = fileWriter.WriteString(sqlBuf.String() + ";\n\n")
		if err != nil {
			return tableInfo, fmt.Errorf("write into SQL failed: %v", err)
		}
	}

	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate table split index region total time", endTime.Sub(startTime).String()),
	)

	// generate table data region range SQL
	// show table regions include data region numbers and index region numbers
	startTime = time.Now()
	query = fmt.Sprintf("show table `%s`.`%s` regions;", tableInfo.DbName, tableInfo.TableName)
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})

	totalRegionCount := 0
	err = queryRows(engine.DB, query, func(row, cols []string) error {

		totalRegionCount++
		return nil
	})
	if err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}

	// calc data region counts
	tableRegionCount := totalRegionCount - indexTotalRegionCount
	if tableRegionCount == 0 {
		return tableInfo, fmt.Errorf("generate table data region range count value equal to 0，show table [%s.%s] data and index data are located in a region, need not split, please manual check", tableInfo.DbName, tableInfo.TableName)
	}
	step := totalRowCount / tableRegionCount

	if step == 0 {
		return tableInfo, fmt.Errorf("table [%s.%s] handle row step is 0, can not probe record region range\n", tableInfo.DbName, tableInfo.TableName)
	}
	query = fmt.Sprintf("select `%[1]s` from (select row_number() over (order by `%[1]s`) as row_num,`%[1]s` from `%[2]s`.`%[3]s` order by `%[1]s`) as t where t.row_num %% %[4]d = 0", tableInfo.HandleName, tableInfo.DbName, tableInfo.TableName, step)
	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` by ", tableInfo.DbName, tableInfo.TableName))
	cnt := 0
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})

	if err = queryRows(engine.DB, query, func(row, cols []string) error {
		if cnt > 0 {
			sqlBuf.WriteString(",")
		}
		cnt++
		sqlBuf.WriteString("('")
		sqlBuf.WriteString(strings.Join(row, "','"))
		sqlBuf.WriteString("')")
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}
	_, err = fileWriter.WriteString(sqlBuf.String() + ";\n\n")
	if err != nil {
		return tableInfo, fmt.Errorf("write into SQL failed: %v", err)
	}

	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate table split data region total time", endTime.Sub(startTime).String()),
	)

	// SQL output flush to file
	if err := fileWriter.Flush(); err != nil {
		return tableInfo, fmt.Errorf("split range region SQL wirte into [%v] failed: %v", fmt.Sprintf("%s/split_range_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}
	if err := file.Close(); err != nil {
		return tableInfo, fmt.Errorf("os file [%v] close failed: %v", fmt.Sprintf("%s/split_range_%s.SQL", tableInfo.OutDir, tableInfo.TableName), err)
	}

	return tableInfo, nil
}
