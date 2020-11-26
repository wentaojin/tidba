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
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/twotwotwo/sorts/sortutil"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/util"
	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"
)

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
	Split range
*/
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

/*
	Split estimate
*/

// splitEstimateTableRun is used to generate split table estimate
func splitEstimateTableRun(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, newIndexName string,
	estimateTableRows, estimateTableSize, regionSize int, concurrency int, outDir string) (EstimateTableInfo, error) {
	var tableInfo EstimateTableInfo
	tableInfo.DbName = dbName
	tableInfo.IndexName = newIndexName
	tableInfo.OutDir = outDir

	// query column distinct value
	startTime := time.Now()
	// string set
	colValues := util.NewStringSet()

	// because sql run slow, program delete repeat
	//query := fmt.Sprintf(`SELECT DISTINCT
	//%s
	//FROM
	//%s.%s`, columnName, dbName, tableName)
	query := fmt.Sprintf(`SELECT
	%s
	FROM
	%s.%s`, columnName, dbName, tableName)
	tableInfo.SQL = append(tableInfo.SQL, SqlInfo{
		SQL: query,
	})
	if err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			return fmt.Errorf("table [%s.%s] is not column values, should never happen", dbName, tableName)
		}
		colValues.Add(row[0])
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}
	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get table column all values total time", endTime.Sub(startTime).String()),
	)

	// because sql not distinct, so need unique colsValue
	var valueCols []string

	// todo: need speed
	startTime = time.Now()
	valueCols = colValues.SortList()
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("unique origin col values total time", endTime.Sub(startTime).String()),
	)

	// number of samples for each unique value
	colCounts := math.Ceil(float64(estimateTableRows) / float64(len(valueCols)))

	// fill the estimate table rows， approximately equal to estimateTableRows
	startTime = time.Now()
	// split array
	var (
		colsInfo          [][]string
		splitColsInfoNums int
	)

	// concurrency limits
	// Less than 2048000, processed at once
	if len(valueCols) <= concurrency {
		// log record
		zlog.Logger.Fatal("Run task info",
			zap.String("appear error", fmt.Sprintf(`origin cols distinct value [%d] is not equal to concurrency value [%d], not need split region or reduce flag concurrency value`, len(valueCols), concurrency)),
		)
	} else {
		// split array
		splitColsInfoNums = int(math.Ceil(float64(len(valueCols)) / float64(concurrency)))
		colsInfo = make([][]string, splitColsInfoNums)
		// paginate offset
		offset := 0
		for i := 0; i < splitColsInfoNums; i++ {
			colsInfo[i] = append(colsInfo[i], util.Paginate(valueCols, offset, concurrency)...)
			offset = offset + concurrency
		}
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("split origin cols value total time", endTime.Sub(startTime).String()),
	)

	// store column values include repeat
	var (
		newCols []string
	)
	// origin append
	//for j := 0; j < int(colCounts); j++ {
	//	newCols = append(newCols, colsValue...)
	//}
	// todo: need speed
	var wg sync.WaitGroup
	c := make(chan struct{})

	startTime = time.Now()
	// after the job is created, the job is ready to receive data from the channel
	s := util.NewScheduleJob(len(colsInfo), int(colCounts), func() { c <- struct{}{} })
	wg.Add(len(colsInfo))
	for i := 0; i < len(colsInfo); i++ {
		go func(v []string) {
			defer wg.Done()
			s.AddData(v)
		}(colsInfo[i])
	}
	wg.Wait()
	s.Close()
	<-c
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate new cols total time", endTime.Sub(startTime).String()),
	)

	newCols = s.Data

	// sort newCols order by asc
	startTime = time.Now()
	sortutil.Strings(newCols) // or sortutil.Bytes([][]byte)
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("sort new cols total time", endTime.Sub(startTime).String()),
	)

	// determine whether the sorted value is equal to before
	totalCols := len(valueCols) * int(colCounts)
	if len(newCols) != totalCols {
		// log record
		zlog.Logger.Fatal("Run task info",
			zap.String("appear error", fmt.Sprintf("sort new cols value [%d] is not equal to origin value [%d]", len(newCols), totalCols)),
		)
	}
	// estimate split region numbers - counts
	regionCounts := math.Ceil(float64(estimateTableSize) / float64(regionSize))

	// rounded up, for example: 2.5 = 3
	// get avg one region store how much table rows data
	oneRegionStep := math.Ceil(float64(len(newCols)) / regionCounts)

	// split array by segments
	splitNums := math.Ceil(float64(len(newCols)) / oneRegionStep)

	// log record
	zlog.Logger.Info("Run task info",
		zap.String("distinct min value", valueCols[0]),
		zap.String("distinct max value", valueCols[len(valueCols)-1]),
		zap.Int("distinct value nums", len(valueCols)),
		zap.Int("factor", int(colCounts)),
		zap.Int("estimate rows", len(newCols)),
		zap.Float64("estimate split region nums", regionCounts),
		zap.Float64("one region rows", oneRegionStep),
		zap.Float64("real split region nums", splitNums),
	)

	// split array
	startTime = time.Now()
	var splitInfo []string
	// paginate offset
	offset := 0
	for i := 0; i < int(splitNums); i++ {
		// take the first value in each array, that ie used to region start key
		// for example:
		// [1,2,3]
		// [4,5,6]
		// [6,7,8]
		// would get 1、4、6
		// finally splicing split table index sql
		splitInfo = append(splitInfo, util.Paginate(newCols, offset, int(oneRegionStep))[0])
		offset = offset + int(oneRegionStep)
	}
	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("generate split info total time", endTime.Sub(startTime).String()),
	)

	// init file output dir and file write obj
	if err := initOrCreateDir(tableInfo.OutDir); err != nil {
		return tableInfo, err
	}

	var (
		fileWriter *bufio.Writer
		file       *os.File
	)

	if newTableName != "" {
		tableInfo.TableName = newTableName
	} else {
		tableInfo.TableName = tableName
	}
	outFile, err := os.OpenFile(fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return tableInfo, fmt.Errorf("open file [%s] failed: %v", fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}
	fileWriter, file = bufio.NewWriter(outFile), outFile

	// splicing sql
	startTime = time.Now()
	sqlBuf := bytes.NewBuffer(nil)
	switch {
	case newDbName == "" && newTableName == "":
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", dbName, tableName, newIndexName))
	case newDbName != "" && newTableName == "":
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", newDbName, tableName, newIndexName))
	case newDbName != "" && newTableName != "":
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", newDbName, newTableName, newIndexName))
	case newDbName == "" && newTableName != "":
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", dbName, newTableName, newIndexName))
	default:
		return tableInfo, fmt.Errorf("flag params parse failed: params not support")
	}
	for _, info := range splitInfo {
		//if info == "" {
		//	return tableInfo, fmt.Errorf("split info splicing failed: column value exist null value or null string")
		//}
		//sqlBuf.WriteString("('")
		//sqlBuf.WriteString(info)
		//sqlBuf.WriteString("')")
		//sqlBuf.WriteString(",")
		// todo: fixed column exist null string
		if info != "" {
			sqlBuf.WriteString("('")
			sqlBuf.WriteString(info)
			sqlBuf.WriteString("')")
			sqlBuf.WriteString(",")
		}
	}

	_, err = fileWriter.WriteString(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n")
	if err != nil {
		return tableInfo, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}

	endTime = time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("splicing sql text total time", endTime.Sub(startTime).String()),
	)

	// SQL output flush to file
	if err := fileWriter.Flush(); err != nil {
		return tableInfo, fmt.Errorf("split range region SQL wirte into [%v] failed: %v",
			fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}
	if err := file.Close(); err != nil {
		return tableInfo, fmt.Errorf("os file [%v] close failed: %v",
			fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}

	return tableInfo, nil
}

/*
	Split key
*/
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
