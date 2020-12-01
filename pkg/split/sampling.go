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
	"time"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/util"
	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"
)

func IncludeTableSplitSampling(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, indexName string,
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

	tableInfo, err := runSplitTableSampling(engine, dbName, tableName, columnName, newDbName, newTableName, indexName, estimateTableRows, estimateTableSize, regionSize, concurrency, outDir)
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
	Split sampling
*/

// split table region by sampling
type SamplingTableInfo struct {
	DbName    string
	TableName string
	IndexName string
	SQL       []SqlInfo
	OutDir    string
	CostTime  string
}

func runSplitTableSampling(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, indexName string,
	estimateTableRows, estimateTableSize, regionSize int, concurrency int, outDir string) (SamplingTableInfo, error) {
	var (
		tableInfo SamplingTableInfo
		err       error
	)
	startTime := time.Now()
	tableInfo, err = splitSamplingTableRun(engine, dbName, tableName, columnName, newDbName, newTableName, indexName, estimateTableRows, estimateTableSize, regionSize, concurrency, outDir)
	endTime := time.Now()
	if err != nil {
		tableInfo.CostTime = endTime.Sub(startTime).String()
		return tableInfo, err
	} else {
		tableInfo.CostTime = endTime.Sub(startTime).String()
	}
	return tableInfo, nil
}

// splitSamplingTableRun is used to generate split table sampling
func splitSamplingTableRun(engine *db.Engine, dbName string, tableName string, columnName string, newDbName, newTableName, newIndexName string,
	estimateTableRows, estimateTableSize, regionSize int, concurrency int, outDir string) (SamplingTableInfo, error) {
	var tableInfo SamplingTableInfo
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

	// estimate split region numbers - counts
	regionCounts := math.Ceil(float64(estimateTableSize) / float64(regionSize))

	// rounded up, for example: 2.5 = 3
	// get avg one region store how much table rows data
	newTableRows := len(valueCols) * int(colCounts)
	oneRegionStep := math.Ceil(float64(newTableRows) / regionCounts)

	// split array by segments
	splitNums := math.Ceil(float64(newTableRows) / oneRegionStep)

	// log record
	zlog.Logger.Info("Run task info",
		zap.String("distinct min value", valueCols[0]),
		zap.String("distinct max value", valueCols[len(valueCols)-1]),
		zap.Int("distinct value nums", len(valueCols)),
		zap.Int("factor", int(colCounts)),
		zap.Int("estimate rows", newTableRows),
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
			fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}
	if err := file.Close(); err != nil {
		return tableInfo, fmt.Errorf("os file [%v] close failed: %v",
			fmt.Sprintf("%s/split_index_%s_%s.SQL", tableInfo.OutDir, tableInfo.TableName, tableInfo.IndexName), err)
	}

	return tableInfo, nil
}
