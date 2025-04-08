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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twotwotwo/sorts/sortutil"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/logger"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

type TableInfo struct {
	Mutex       *sync.Mutex
	DbName      string
	TableName   string
	Indexes     []*IndexInfo
	HandleKey   []string
	Sqls        []string
	OutDir      string
	Cost        float64
	IsClustered bool
}

func (t *TableInfo) appendSql(sql string) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.Sqls = append(t.Sqls, sql)
}

type IndexInfo struct {
	IndexName  string
	ColumnName []string
}

type Task struct {
	Mutex     *sync.Mutex   `json:"-"`
	file      *os.File      `json:"-"`
	writer    *bufio.Writer `json:"-"`
	Command   string
	DbName    string
	TableName string
	OutDir    string
	Engine    *mysql.Database
}

func (t *Task) Init() error {
	outCompFile, err := os.OpenFile(filepath.Join(t.OutDir, fmt.Sprintf("split_%s_%s_%s.sql", strings.ToLower(t.Command), strings.ToLower(t.DbName), strings.ToLower(t.TableName))), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	t.writer, t.file = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (t *Task) Write(str string) (int, error) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.writer.WriteString(str)
}

func (t *Task) Close() error {
	if t.file != nil {
		err := t.writer.Flush()
		if err != nil {
			return err
		}
		err = t.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Task) RangeCommand(ctx context.Context) (*TableInfo, error) {
	startTime := time.Now()
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split range sql start...", t.DbName, t.TableName))

	if err := t.Init(); err != nil {
		return nil, err
	}
	defer t.Close()

	tableInfo := &TableInfo{
		Mutex: &sync.Mutex{},
	}

	tableInfo.DbName = t.DbName
	tableInfo.TableName = t.TableName
	tableInfo.OutDir = t.OutDir

	// get database table information
	queryTime := time.Now()
	// get database table index all column value
	queryStr := fmt.Sprintf(`SELECT 
    KEY_NAME,
    CLUSTERED,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_NAMES
FROM 
    INFORMATION_SCHEMA.TIDB_INDEXES
WHERE 
    table_name = '%s'
    AND table_schema = '%s'
GROUP BY 
    KEY_NAME, CLUSTERED`, t.TableName, t.DbName)

	tableInfo.appendSql(queryStr)

	_, res, err := t.Engine.GeneralQuery(ctx, queryStr)
	if err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	for _, r := range res {
		if strings.EqualFold(r["KEY_NAME"], "PRIMARY") {
			columns := strings.Split(r["COLUMN_NAMES"], "|+|")
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
			if strings.EqualFold(r["CLUSTERED"], "YES") {
				tableInfo.IsClustered = true
				tableInfo.HandleKey = columns
			} else {
				tableInfo.IsClustered = false
				tableInfo.HandleKey = append(tableInfo.HandleKey, "_tidb_rowid")
			}
		} else {
			columns := strings.Split(r["COLUMN_NAMES"], "|+|")
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
		}
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] index columns query finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	if len(tableInfo.HandleKey) == 0 {
		return tableInfo, fmt.Errorf("can not found the handle column of table %s.%s", t.DbName, t.TableName)
	}

	// query the database table row counts
	queryTime = time.Now()
	totalRowCount := 0

	queryStr = fmt.Sprintf("SELECT COUNT(1) FROM `%s`.`%s`", t.DbName, t.TableName)
	tableInfo.appendSql(queryStr)

	if err = t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
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
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] row counts query finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	if totalRowCount == 0 {
		return tableInfo, fmt.Errorf("table [%s.%s] row count is 0, can not probe region range", tableInfo.DbName, tableInfo.TableName)
	}

	// 	generate table index region range sql
	queryTime = time.Now()
	indexTotalRegionCounts := 0

	for _, ind := range tableInfo.Indexes {
		// the primary key index split can only be run on non-clustered tables.
		// An error will be reported if it is run on clustered tables. Error example: ERROR 1176 (42000): Key 'PRIMARY' doesn't exist in table 't13'
		if strings.EqualFold(ind.IndexName, "PRIMARY") && tableInfo.IsClustered {
			continue
		}
		indexTime := time.Now()
		// get index regions (show table regions)
		queryStr = fmt.Sprintf("SHOW TABLE `%s`.`%s` INDEX `%s` REGIONS", tableInfo.DbName, tableInfo.TableName, ind.IndexName)
		tableInfo.appendSql(queryStr)

		count := 0
		err := t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
			count++
			return nil
		})
		if err != nil {
			return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
		}
		indexTotalRegionCounts += count

		if count == 0 {
			return tableInfo, fmt.Errorf("generate database table [%s.%s] index [%s] region range count value equal to 0，integer divide by zero", tableInfo.DbName, tableInfo.TableName, ind.IndexName)
		}

		step := totalRowCount / count
		if step == 0 {
			logger.Info(fmt.Sprintf("Database table [%s.%s] index [%s] step is zero, can not probe index region range, skip...", t.DbName, t.TableName, ind.IndexName))
			continue
		}

		// rownum get split region value
		queryStr = fmt.Sprintf("select `%[1]s` from (select row_number() over (order by `%[1]s`) as row_num,`%[1]s` from `%[2]s`.`%[3]s` order by `%[1]s`) as t where t.row_num %% %[4]d = 0", strings.Join(ind.ColumnName, "`,`"), tableInfo.DbName, tableInfo.TableName, step)

		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", tableInfo.DbName, tableInfo.TableName, ind.IndexName))

		tableInfo.appendSql(queryStr)

		cnt := 0

		if err = t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
			if cnt > 0 {
				sqlBuf.WriteString(",")
			}
			cnt++
			sqlBuf.WriteString("(")
			sqlBuf.WriteString(strings.Join(row, ","))
			sqlBuf.WriteString(")")
			return nil
		}); err != nil {
			return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
		}
		_, err = t.Write(sqlBuf.String() + ";\n\n")
		if err != nil {
			return tableInfo, fmt.Errorf("the database table [%s.%s] write into sqlfile [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
		}

		logger.Info(fmt.Sprintf("Database table [%s.%s] generate index [%s] split sql finished in %fs", t.DbName, t.TableName, ind.IndexName, time.Since(indexTime).Seconds()))
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate index split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	// generate table data region range SQL
	// show table regions include data region numbers and index region numbers
	queryTime = time.Now()
	queryStr = fmt.Sprintf("SHOW TABLE `%s`.`%s` REGIONS", tableInfo.DbName, tableInfo.TableName)
	tableInfo.appendSql(queryStr)

	totalRegionCount := 0
	err = t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
		totalRegionCount++
		return nil
	})
	if err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	// calc data region counts
	tableRegionCount := totalRegionCount - indexTotalRegionCounts
	if tableRegionCount == 0 {
		return tableInfo, fmt.Errorf("generate table data region range count value equal to 0，show table [%s.%s] data and index data are located in a region, need not split, please manual check", tableInfo.DbName, tableInfo.TableName)
	}
	step := totalRowCount / tableRegionCount

	if step == 0 {
		return tableInfo, fmt.Errorf("the database table [%s.%s] handle row step is 0, can not probe record region range", tableInfo.DbName, tableInfo.TableName)
	}
	queryStr = fmt.Sprintf("select `%[1]s` from (select row_number() over (order by `%[1]s`) as row_num,`%[1]s` from `%[2]s`.`%[3]s` order by `%[1]s`) as t where t.row_num %% %[4]d = 0", strings.Join(tableInfo.HandleKey, ","), tableInfo.DbName, tableInfo.TableName, step)

	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` by ", tableInfo.DbName, tableInfo.TableName))

	cnt := 0

	tableInfo.appendSql(queryStr)

	if err = t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
		if cnt > 0 {
			sqlBuf.WriteString(",")
		}
		cnt++
		sqlBuf.WriteString("(")
		sqlBuf.WriteString(strings.Join(row, ","))
		sqlBuf.WriteString(")")
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}
	_, err = t.Write(sqlBuf.String() + ";\n\n")
	if err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] write into sqlfile [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate data split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	cost := time.Since(startTime).Seconds()

	tableInfo.Cost = cost

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split range sql finished in %fs", t.DbName, t.TableName, cost))

	return tableInfo, nil
}

func (t *Task) KeyCommand(ctx context.Context) (*TableInfo, error) {
	startTime := time.Now()
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split key sql start...", t.DbName, t.TableName))

	if err := t.Init(); err != nil {
		return nil, err
	}
	defer t.Close()

	tableInfo := &TableInfo{
		Mutex: &sync.Mutex{},
	}

	tableInfo.DbName = t.DbName
	tableInfo.TableName = t.TableName
	tableInfo.OutDir = t.OutDir

	// get database table information
	queryTime := time.Now()

	queryStr := fmt.Sprintf(`SELECT 
    KEY_NAME,
    CLUSTERED,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_NAMES
FROM 
    INFORMATION_SCHEMA.TIDB_INDEXES
WHERE 
    table_name = '%s'
    AND table_schema = '%s'
GROUP BY 
    KEY_NAME, CLUSTERED`, t.TableName, t.DbName)

	tableInfo.appendSql(queryStr)

	_, res, err := t.Engine.GeneralQuery(ctx, queryStr)
	if err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	indexKeyColumn := make(map[string][]string)
	for _, r := range res {
		columns := strings.Split(r["COLUMN_NAMES"], "|+|")
		indexKeyColumn[r["KEY_NAME"]] = columns

		if strings.EqualFold(r["KEY_NAME"], "PRIMARY") {
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
			if strings.EqualFold(r["CLUSTERED"], "YES") {
				tableInfo.IsClustered = true
				tableInfo.HandleKey = columns
			} else {
				tableInfo.IsClustered = false
				tableInfo.HandleKey = append(tableInfo.HandleKey, "_tidb_rowid")
			}
		} else {
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
		}
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] index columns query finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	if len(tableInfo.HandleKey) == 0 {
		return tableInfo, fmt.Errorf("can not found the handle column of table %s.%s", t.DbName, t.TableName)
	}

	// get database table data start_key
	// decode region keys
	queryStr = fmt.Sprintf(`SELECT
	tidb_decode_key(start_key) AS dec_start_key
FROM
	information_schema.tikv_region_status 
WHERE
	db_name = '%s'
	AND table_name = '%s'
	AND is_index = 0 
ORDER BY
	start_key`, t.DbName, t.TableName)

	tableInfo.appendSql(queryStr)

	var columnKeyValues []string
	totalKeyCounts := 0
	if err := t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
		if len(row) != 1 {
			return fmt.Errorf("the database table [%s.%s] is not reocrd, should never happen", t.DbName, t.TableName)
		}
		columnKeyValues = append(columnKeyValues, strings.Trim(row[0], "'"))
		totalKeyCounts++
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] data start_key query finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	if totalKeyCounts <= 10 {
		return tableInfo, fmt.Errorf("the database table [%s.%s] region nums is lower than 10, it's needn't split", t.DbName, t.TableName)
	}

	// exclude max region key, because max key can't decode
	// generate split data region sql file
	queryTime = time.Now()
	// When the number of data key regions exceeds 1024, 1024 keys are randomly selected. If the number does not exceed 1024, the actual number is used.
	keyValues, err := stringutil.RandomSampleStringSlice(columnKeyValues[1:], 1024)
	if err != nil {
		return tableInfo, err
	}

	// Determine whether it is a clustered table. If the data contains _tidb_rowid, it means it is a non-clustered table.
	// {"_tidb_rowid":1958897,"table_id":"59"} -> NonClustered
	// {"handle":{"a":"6","id":"c4038db2-d51c-11eb-8c75-80e65018a9be"},"table_id":62} -> StringClustered
	// {"id":9504342,"table_id":"942"} -> IntegerClustered
	values := make(map[string]interface{})
	if err := json.Unmarshal([]byte(keyValues[0]), &values); err != nil {
		return nil, err
	}

	var clusteredType string
	clusteredType = "IntegerClustered"

	if _, existed := values["_tidb_rowid"]; existed {
		clusteredType = "NonClustered"
	}
	if _, existed := values["handle"]; existed {
		clusteredType = "StringClustered"
	}

	// If the data table is a non-clustered table, skip the shard_rowid scattering. Normally, shard_rowid_bits is used to scatter the data. Only the Primary index Key data needs to be scattered.
	if clusteredType != "NonClustered" {
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` by ", tableInfo.DbName, tableInfo.TableName))

		pkColumns, existed := indexKeyColumn["PRIMARY"]
		if !existed {
			return nil, fmt.Errorf("the database table [%s.%s] primary key not found, but the table region shows clustering with joint primary key", t.DbName, t.TableName)
		}

		for _, key := range keyValues {
			sqlBuf.WriteString("(")
			values = make(map[string]interface{})
			if err := json.Unmarshal([]byte(key), &values); err != nil {
				return nil, err
			}
			if clusteredType == "StringClustered" {
				var pkValues []string
				for _, p := range pkColumns {
					for k, v := range values["handle"].(map[string]string) {
						if strings.EqualFold(p, k) {
							pkValues = append(pkValues, fmt.Sprintf(`'%s'`, v))
						}
					}
				}
				sqlBuf.WriteString(strings.Join(pkValues, ","))
				sqlBuf.WriteString(")")
				sqlBuf.WriteString(",")
			} else {
				var pkValues []string
				for _, p := range pkColumns {
					for k, v := range values {
						if strings.EqualFold(p, k) {
							pkValues = append(pkValues, fmt.Sprintf(`%v`, v.(int)))
						}
					}
				}
				sqlBuf.WriteString(strings.Join(pkValues, ","))
				sqlBuf.WriteString(")")
				sqlBuf.WriteString(",")
			}
		}

		_, err = t.Write(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n")
		if err != nil {
			return nil, fmt.Errorf("the database table [%s.%s] data split sql file write failed: %v", t.DbName, t.TableName, err)
		}
		logger.Info(fmt.Sprintf("Database table [%s.%s] generate data split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))
	} else {
		logger.Info(fmt.Sprintf("Database table [%s.%s] non-clustered needn't data split, please use SHARD_ROW_ID_BITS scatter, skip finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))
	}

	// generate table index split sql
	queryTime = time.Now()
	for _, ind := range tableInfo.Indexes {
		// the primary key index split can only be run on non-clustered tables.
		// An error will be reported if it is run on clustered tables. Error example: ERROR 1176 (42000): Key 'PRIMARY' doesn't exist in table 't13'
		if strings.EqualFold(ind.IndexName, "PRIMARY") && tableInfo.IsClustered {
			continue
		}
		indexTime := time.Now()
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", tableInfo.DbName, tableInfo.TableName, ind.IndexName))

		queryStr = fmt.Sprintf(`SELECT
		tidb_decode_key(start_key) AS dec_start_key
	FROM
		information_schema.tikv_region_status 
	WHERE
		db_name = '%s'
		AND table_name = '%s'
		AND index_name = '%s' 
		AND is_index = 1
	ORDER BY
		start_key`, t.DbName, t.TableName, ind.IndexName)
		tableInfo.appendSql(queryStr)

		var colIndexValues []string
		if err = t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
			if len(row) != 1 {
				return fmt.Errorf("table [%s.%s] is not index column name, should never happen", t.DbName, t.TableName)
			}
			colIndexValues = append(colIndexValues, strings.Trim(row[0], "'"))
			return nil
		}); err != nil {
			return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
		}

		// exclude max region key, because max key can't split
		// When the number of index key regions exceeds 1024, 1024 keys are randomly selected. If the number does not exceed 1024, the actual number is used.
		keyValues, err := stringutil.RandomSampleStringSlice(colIndexValues[1:], 1024)
		if err != nil {
			return tableInfo, err
		}

		indexColumns, existed := indexKeyColumn[ind.IndexName]
		if !existed {
			return nil, fmt.Errorf("the database table [%s.%s] index [%s] not found column value, not meeting expectations", t.DbName, t.TableName, ind.IndexName)
		}
		for _, key := range keyValues {
			values := make(map[string]interface{})
			if err = json.Unmarshal([]byte(key), &values); err != nil {
				return nil, err
			}

			sqlBuf.WriteString("(")

			var columnVals []string
			for _, i := range indexColumns {
				for k, v := range values["index_vals"].(map[string]string) {
					if strings.EqualFold(i, k) {
						columnVals = append(columnVals, fmt.Sprintf("'%s'", v))
					}
				}
			}
			sqlBuf.WriteString(strings.Join(columnVals, ","))
			sqlBuf.WriteString(")")
			sqlBuf.WriteString(",")
		}

		_, err = t.Write(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n")
		if err != nil {
			return nil, fmt.Errorf("the database table [%s.%s] index [%s] split sql file write failed: %v", t.DbName, t.TableName, ind.IndexName, err)
		}
		logger.Info(fmt.Sprintf("Database table [%s.%s] generate index [%s] split sql finished in %fs", t.DbName, t.TableName, ind.IndexName, time.Since(indexTime).Seconds()))
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate index split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	cost := time.Since(startTime).Seconds()

	tableInfo.Cost = cost

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split key sql finished in %fs", t.DbName, t.TableName, cost))
	return tableInfo, nil
}

func (t *Task) SamplingCommand(ctx context.Context, indexName, newDB, newTable, newIndex, resource string, estimates int) (*TableInfo, error) {
	startTime := time.Now()
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split sampling sql start...", t.DbName, t.TableName))

	if err := t.Init(); err != nil {
		return nil, err
	}
	defer t.Close()

	tableInfo := &TableInfo{
		Mutex: &sync.Mutex{},
	}

	tableInfo.DbName = t.DbName
	tableInfo.TableName = t.TableName
	tableInfo.OutDir = t.OutDir

	// get database table information
	queryTime := time.Now()

	queryStr := fmt.Sprintf(`SELECT 
	KEY_NAME,
	CLUSTERED,
	GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_NAMES
FROM 
	INFORMATION_SCHEMA.TIDB_INDEXES
WHERE 
	table_name = '%s'
	AND table_schema = '%s'
	AND key_name = '%s'
GROUP BY 
	KEY_NAME, CLUSTERED`, t.TableName, t.DbName, indexName)

	tableInfo.appendSql(queryStr)

	_, res, err := t.Engine.GeneralQuery(ctx, queryStr)
	if err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}

	indexKeyColumn := make(map[string][]string)
	for _, r := range res {
		columns := strings.Split(r["COLUMN_NAMES"], "|+|")
		indexKeyColumn[r["KEY_NAME"]] = columns

		if strings.EqualFold(r["KEY_NAME"], "PRIMARY") {
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
			if strings.EqualFold(r["CLUSTERED"], "YES") {
				tableInfo.IsClustered = true
				tableInfo.HandleKey = columns
			} else {
				tableInfo.IsClustered = false
				tableInfo.HandleKey = append(tableInfo.HandleKey, "_tidb_rowid")
			}
		} else {
			tableInfo.Indexes = append(tableInfo.Indexes, &IndexInfo{
				IndexName:  r["KEY_NAME"],
				ColumnName: columns,
			})
		}
	}

	logger.Info(fmt.Sprintf("Database table [%s.%s] index columns query finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	if len(tableInfo.Indexes) == 0 {
		return nil, fmt.Errorf("the database table [%s.%s] not found index [%s]", t.DbName, t.TableName, indexName)
	}

	if strings.EqualFold(resource, "database") {
		var (
			indDistValues [][]string
			regionCounts  float64
		)

		// get index column distinct value
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(2)
		g.Go(func() error {
			sTime := time.Now()
			indColumns := strings.Join(tableInfo.Indexes[0].ColumnName, ",")
			querySql := fmt.Sprintf("SELECT DISTINCT %s FROM `%s`.`%s` ORDER BY %s",
				indColumns, t.DbName, t.TableName, indColumns)
			tableInfo.appendSql(querySql)

			if err := t.Engine.QueryRows(gCtx, querySql, func(row, cols []string) error {
				indDistValues = append(indDistValues, row)
				return nil
			}); err != nil {
				return fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
			}
			logger.Info(fmt.Sprintf("Database table [%s.%s] query index column distinct values finished in %fs", t.DbName, t.TableName, time.Since(sTime).Seconds()))
			return nil
		})

		// calculate index split region nums
		g.Go(func() error {
			var err error
			regionCounts, err = t.calculateRegions(gCtx, tableInfo, estimates)
			if err != nil {
				return err
			}
			return nil
		})

		if err := g.Wait(); err != nil {
			return nil, err
		}

		// generate index split region
		queryTime = time.Now()
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", newDB, newTable, newIndex))

		step := len(indDistValues) / int(regionCounts)
		if step < 1 {
			step = 1
		}
		for i := 0; i < len(indDistValues); i += step {
			if i > 0 {
				sqlBuf.WriteString(",")
			}
			vs := indDistValues[i]
			sqlBuf.WriteString("(")
			sqlBuf.WriteString(strings.Join(vs, ","))
			sqlBuf.WriteString(")")
		}
		_, err = t.Write(sqlBuf.String() + ";\n\n")
		if err != nil {
			return nil, err
		}
		logger.Info(fmt.Sprintf("Database table [%s.%s] generate index split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

		cost := time.Since(startTime).Seconds()

		tableInfo.Cost = cost

		logger.Info(fmt.Sprintf("Database table [%s.%s] generate split sampling sql finished in %fs", t.DbName, t.TableName, cost))
		return tableInfo, nil
	}

	// get database table index distinct counts
	var (
		indColumnDistCounts int
		regionCounts        float64
	)
	indColumns := strings.Join(tableInfo.Indexes[0].ColumnName, ",")

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error {
		sTime := time.Now()
		querySql := fmt.Sprintf("SELECT COUNT(DISTINCT %s) AS COUNT FROM `%s`.`%s` ORDER BY %s",
			indColumns, t.DbName, t.TableName, indColumns)

		tableInfo.appendSql(querySql)
		_, res, err := t.Engine.GeneralQuery(gCtx, querySql)
		if err != nil {
			return nil
		}
		val, err := strconv.Atoi(res[0]["COUNT"])
		if err != nil {
			return err
		}
		indColumnDistCounts = val
		logger.Info(fmt.Sprintf("Database table [%s.%s] query index distinct counts [%d] finished in %fs", t.DbName, t.TableName, val, time.Since(sTime).Seconds()))
		return nil
	})

	// calculate index split region nums
	g.Go(func() error {
		var err error
		regionCounts, err = t.calculateRegions(gCtx, tableInfo, estimates)
		if err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// generate index split region
	queryTime = time.Now()
	step := math.Ceil(float64(indColumnDistCounts) / regionCounts)
	if step < 1 {
		step = 1
	}

	// get region segmentation boundary value by paginate sql
	queryStr = fmt.Sprintf(`SELECT
	%[1]s 
FROM
	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT DISTINCT %[1]s FROM %[2]s ) AS S ) AS T 
WHERE
	RowNumber %% %[3]d = 1`, indColumns, fmt.Sprintf("`%s`.`%s`", t.DbName, t.TableName), int(step))
	tableInfo.appendSql(queryStr)

	var indDistValues [][]string
	if err := t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
		indDistValues = append(indDistValues, row)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] query index column distinct values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	// generate split index sql
	queryTime = time.Now()
	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", newDB, newTable, newIndex))

	for i, values := range indDistValues {
		if i > 0 {
			sqlBuf.WriteString(",")
		}
		sqlBuf.WriteString("(")
		sqlBuf.WriteString(strings.Join(values, ","))
		sqlBuf.WriteString(")")
	}
	_, err = t.Write(sqlBuf.String() + ";\n\n")
	if err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate index split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	cost := time.Since(startTime).Seconds()
	tableInfo.Cost = cost
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split key sql finished in %fs", t.DbName, t.TableName, cost))
	return tableInfo, nil
}

func (t *Task) calculateRegions(ctx context.Context, tableInfo *TableInfo, estimates int) (float64, error) {
	calTime := time.Now()
	sTime := time.Now()
	querySql := fmt.Sprintf("SELECT COUNT(1) AS COUNT FROM `%s`.`%s`", t.DbName, t.TableName)
	tableInfo.appendSql(querySql)
	_, res, err := t.Engine.GeneralQuery(ctx, querySql)
	if err != nil {
		return 0, err
	}
	rowCounts, err := strconv.Atoi(res[0]["COUNT"])
	if err != nil {
		return 0, err
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] query table count rows finished in %fs", t.DbName, t.TableName, time.Since(sTime).Seconds()))

	sTime = time.Now()
	querySql = fmt.Sprintf("SHOW TABLE `%s`.`%s` INDEX `%s` REGIONS", t.DbName, t.TableName, tableInfo.Indexes[0].IndexName)
	indexRegions := 0
	if err := t.Engine.QueryRows(ctx, querySql, func(row, cols []string) error {
		indexRegions++
		return nil
	}); err != nil {
		return 0, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, querySql, err)
	}

	if indexRegions < 1 {
		indexRegions = 1
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] query index [%s] region counts finished in %fs", t.DbName, t.TableName, tableInfo.Indexes[0].IndexName, time.Since(sTime).Seconds()))

	capacity := rowCounts / indexRegions
	// To prevent too little data stored in the region, generally a region can store 10,000 indexes,
	// Because too many regions are not good
	if capacity < 10000 {
		capacity = 10000
	}

	regionCounts := math.Ceil(float64(estimates) / float64(capacity))
	if regionCounts < 1 {
		regionCounts = 1
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] calculate index region counts [%v] finished in %fs", t.DbName, t.TableName, regionCounts, time.Since(calTime).Seconds()))

	return regionCounts, nil
}

func (t *Task) EstimateCommand(ctx context.Context, columnNames []string, newDB, newTable, newIndex string, estimateRow, estimateSize, concurrency, regionSize int) (*TableInfo, error) {
	startTime := time.Now()
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split sampling sql start...", t.DbName, t.TableName))

	if err := t.Init(); err != nil {
		return nil, err
	}
	defer t.Close()

	tableInfo := &TableInfo{
		Mutex: &sync.Mutex{},
	}

	tableInfo.DbName = t.DbName
	tableInfo.TableName = t.TableName
	tableInfo.OutDir = t.OutDir

	// get database table information
	queryTime := time.Now()
	con := stringutil.NewContainer()

	columns := strings.Join(columnNames, ",")

	// database sql run slow, program delete repeat
	//queryStr := fmt.Sprintf(`SELECT DISTINCT
	//%s
	//FROM
	//%s.%s`, columnName, dbName, tableName)

	// database sql not distinct, so need unique
	queryStr := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", columns, t.DbName, t.TableName)
	tableInfo.Sqls = append(tableInfo.Sqls, queryStr)

	if err := t.Engine.QueryRows(ctx, queryStr, func(row, cols []string) error {
		con.Add(strings.Join(row, ","))
		return nil
	}); err != nil {
		return tableInfo, fmt.Errorf("the database table [%s.%s] run sql [%s] failed: %v", t.DbName, t.TableName, queryStr, err)
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] query esitmate column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	// return order string
	var sortConVals []string
	queryTime = time.Now()
	sortConVals = con.SortList()
	logger.Info(fmt.Sprintf("Database table [%s.%s] sort and deduplication esitmate column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	sortConValCounts := len(sortConVals)
	// calculate the number of distinct values ​​for each based on the estimated number of rows written
	columnDataCounts := math.Ceil(float64(estimateRow) / float64(sortConValCounts))

	// fill the estimate table rows， approximately equal to estimateTableRows
	var (
		colsInfo          [][]string
		splitColsInfoNums int
	)
	// split array
	splitColsInfoNums = int(math.Ceil(float64(sortConValCounts) / float64(concurrency)))
	colsInfo = make([][]string, splitColsInfoNums)
	// paginate offset
	offset := 0
	for i := 0; i < splitColsInfoNums; i++ {
		colsInfo[i] = append(colsInfo[i], stringutil.Paginate(sortConVals, offset, concurrency)...)
		offset = offset + concurrency
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] query esitmate column value counts [%d] finished in %fs", t.DbName, t.TableName, sortConValCounts, time.Since(queryTime).Seconds()))
	logger.Info(fmt.Sprintf("Database table [%s.%s] split paginate esitmate column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	// store column values include repeat(fill)
	queryTime = time.Now()
	ca := stringutil.NewConcurrentArray()

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, col := range colsInfo {
		c := col
		g.Go(func() error {
			ca.Append(c)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate new estimate column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	// resort newColumnVals order by asc
	queryTime = time.Now()
	var newColumnVals []string
	for _, c := range ca.All() {
		newColumnVals = append(newColumnVals, c.([]string)...)
	}
	sortutil.Strings(newColumnVals)
	logger.Info(fmt.Sprintf("Database table [%s.%s] sort new esitmate column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	// determine whether the sorted value is equal to before
	totalCols := len(sortConVals) * int(columnDataCounts)
	newColumnsCounts := len(newColumnVals)
	if newColumnsCounts != totalCols {
		return nil, fmt.Errorf("the database table [%s.%s] sort new column value counts [%d] is not equal to estimate column value counts [%d]", t.DbName, t.TableName, newColumnsCounts, totalCols)
	}
	// estimate split region numbers - counts
	regionCounts := math.Ceil(float64(estimateSize) / float64(regionSize))

	// rounded up, for example: 2.5 = 3
	// get avg one region store how much table rows data
	oneRegionStep := math.Ceil(float64(newColumnsCounts) / regionCounts)

	// split array by segments
	splitNums := math.Ceil(float64(newColumnsCounts) / oneRegionStep)

	logger.Info(fmt.Sprintf("Database table [%s.%s] generate new estimate column value counts [%d] finished in %fs", t.DbName, t.TableName, newColumnsCounts, time.Since(queryTime).Seconds()))

	logger.Info(fmt.Sprintf("Database table [%s.%s] estimate new region counts [%f] region step [%f] split nums [%f] finished in %fs", t.DbName, t.TableName, regionCounts, oneRegionStep, splitNums, time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	var splitRegionInfos []string
	// paginate offset
	offset = 0
	for i := 0; i < int(splitNums); i++ {
		// take the first value in each array, that ie used to region start key
		// for example:
		// [1 2 3]
		// [4 5 6]
		// [6 7 8]
		// would get 1、4、6
		// finally splicing split table index sql
		splitRegionInfos = append(splitRegionInfos, stringutil.Paginate(newColumnVals, offset, int(oneRegionStep))[0])
		offset = offset + int(oneRegionStep)
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate new region start_key column values finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString(fmt.Sprintf("split table `%s`.`%s` index `%s` by ", newDB, newTable, newIndex))

	for _, info := range splitRegionInfos {
		// fixed column exist null string, if the column value exist null value or null string, skip...
		if info != "" {
			sqlBuf.WriteString("(")
			sqlBuf.WriteString(info)
			sqlBuf.WriteString(")")
			sqlBuf.WriteString(",")
		}
	}
	if _, err := t.Write(strings.TrimRight(sqlBuf.String(), ",") + ";\n\n"); err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate index split sql finished in %fs", t.DbName, t.TableName, time.Since(queryTime).Seconds()))

	cost := time.Since(startTime).Seconds()
	tableInfo.Cost = cost
	logger.Info(fmt.Sprintf("Database table [%s.%s] generate split estimate sql finished in %fs", t.DbName, t.TableName, cost))
	return tableInfo, nil
}
