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
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twotwotwo/sorts/sortutil"

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/util"
)

func IncludeTableSplitEstimate(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, indexName string,
	estimateTableRows, estimateTableSize, regionSize int, concurrency int, outDir string) error {

	var includeTables []string

	includeTables = append(includeTables, tableName)

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db [%s] table '%v' not exist", dbName, notExistTables)
	}

	tableInfo, err := runSplitTableEstimate(engine, dbName, tableName, columnName, newDbName, newTableName, indexName, estimateTableRows, estimateTableSize, regionSize, concurrency, outDir)
	if err != nil {
		zlog.Logger.Fatal("Task execute failed",
			zap.Error(err),
		)
	}
	t, err := json.Marshal(tableInfo)
	if err != nil {
		zlog.Logger.Fatal("Task json table info struct failed",
			zap.String("tableInfo", string(t)),
		)
	}
	zlog.Logger.Info("Task execute success",
		zap.String("tableInfo", string(t)))

	return nil
}

/*
	Split estimate
*/

// split table region by estimate
type EstimateTableInfo struct {
	DbName    string
	TableName string
	IndexName string
	SQL       []SqlInfo
	OutDir    string
	CostTime  string
}

func runSplitTableEstimate(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, indexName string,
	estimateTableRows, estimateTableSize, regionSize int, concurrency int, outDir string) (EstimateTableInfo, error) {
	var (
		tableInfo EstimateTableInfo
		err       error
	)
	startTime := time.Now()
	tableInfo, err = splitEstimateTableRun(engine, dbName, tableName, columnName, newDbName, newTableName, indexName, estimateTableRows, estimateTableSize, regionSize, concurrency, outDir)
	endTime := time.Now()
	if err != nil {
		tableInfo.CostTime = endTime.Sub(startTime).String()
		return tableInfo, err
	} else {
		tableInfo.CostTime = endTime.Sub(startTime).String()
	}
	return tableInfo, nil
}

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
		var rows []string
		for _, r := range row {
			rows = append(rows, fmt.Sprintf(`'%s'`, r))
		}
		colValues.Add(strings.Join(rows, ","))
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
	sortutil.Strings(newCols)
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

		// fixed column exist null string
		if info != "" {
			sqlBuf.WriteString("(")
			sqlBuf.WriteString(info)
			sqlBuf.WriteString(")")
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
			fmt.Sprintf("%s/split_%s_%s_by_estimate.sql", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}
	if err := file.Close(); err != nil {
		return tableInfo, fmt.Errorf("os file [%v] close failed: %v",
			fmt.Sprintf("%s/split_%s_%s_by_estimate.sql", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}

	return tableInfo, nil
}
