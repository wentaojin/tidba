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
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
)

func GenerateSplitByBaseTable(engine *db.Engine, baseDB, baseTable, baseIndex, newDB, newTable, newIndex, outDir string, totalWriteRows int) error {
	// 1. get distinct value.
	s := &splitByBase{
		baseDB:      baseDB,
		baseTable:   baseTable,
		baseIndex:   baseIndex,
		newDB:       newDB,
		newTable:    newTable,
		newIndex:    newIndex,
		outFilePath: path.Join(outDir, fmt.Sprintf("split_%s_%s_by_sampling.sql", baseTable, baseIndex)),
	}
	startTime := time.Now()
	err := s.init(engine)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var err1, err2 error
	var regionCount int

	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = s.getBaseDistinctValues(engine)
	}()

	go func() {
		defer wg.Done()
		regionCount, err2 = s.calculateRegionNum(engine, totalWriteRows)
	}()
	wg.Wait()

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}

	err = s.generateSplit(regionCount)
	if err != nil {
		return err2
	}

	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("generate split table sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

type splitByBase struct {
	baseIndexInfo  IndexInfo
	file           *os.File
	fileWriter     *bufio.Writer
	distinctValues [][]string

	baseDB      string
	baseTable   string
	baseIndex   string
	newDB       string
	newTable    string
	newIndex    string
	outFilePath string
}

func (s *splitByBase) init(engine *db.Engine) error {
	err := s.initOutFile()
	if err != nil {
		return err
	}
	err = s.getBaseTableIndex(engine)
	if err != nil {
		return err
	}

	return nil
}

func (s *splitByBase) generateSplit(regionCount int) error {
	if regionCount < 1 {
		regionCount = 1
	}
	sqlBuf := bytes.NewBuffer(nil)
	switch {
	case s.newDB == "" && s.newTable == "":
		sqlBuf.WriteString(fmt.Sprintf("split table %s index %s by ", s.tableName(s.baseDB, s.baseTable), s.newIndex))
	case s.newDB != "" && s.newTable == "":
		sqlBuf.WriteString(fmt.Sprintf("split table %s index %s by ", s.tableName(s.newDB, s.baseTable), s.newIndex))
	case s.newDB != "" && s.newTable != "":
		sqlBuf.WriteString(fmt.Sprintf("split table %s index %s by ", s.tableName(s.newDB, s.newTable), s.newIndex))
	case s.newDB == "" && s.newTable != "":
		sqlBuf.WriteString(fmt.Sprintf("split table %s index %s by ", s.tableName(s.baseDB, s.newTable), s.newIndex))
	default:
		return fmt.Errorf("flag params parse failed: params not support")
	}

	step := len(s.distinctValues) / regionCount
	if step < 1 {
		step = 1
	}
	for i := 0; i < len(s.distinctValues); i += step {
		if i > 0 {
			sqlBuf.WriteString(",")
		}
		vs := s.distinctValues[i]
		sqlBuf.WriteString("(")
		sqlBuf.WriteString(strings.Join(vs, ","))
		sqlBuf.WriteString(")")
	}

	_, err := s.fileWriter.WriteString(sqlBuf.String() + ";\n\n")
	return err
}

func (s *splitByBase) initOutFile() error {
	outFile, err := os.OpenFile(s.outFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	s.fileWriter, s.file = bufio.NewWriter(outFile), outFile
	return nil
}

func (s *splitByBase) close() {
	if s.file != nil {
		s.fileWriter.Flush()
		s.file.Close()
	}
}

func (s *splitByBase) getBaseTableIndex(engine *db.Engine) error {
	s.baseIndexInfo = IndexInfo{
		IndexName:  s.baseIndex,
		ColumnName: nil,
	}
	condition := fmt.Sprintf("where lower(table_name)=lower('%s') and lower(table_schema)=lower('%s') and lower(KEY_NAME) = lower ('%s') order by SEQ_IN_INDEX",
		s.baseTable, s.baseDB, s.baseIndex)
	query := fmt.Sprintf("select COLUMN_NAME from INFORMATION_SCHEMA.TIDB_INDEXES %s", condition)
	err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			panic("result row is not index column name, should never happen")
		}
		s.baseIndexInfo.ColumnName = append(s.baseIndexInfo.ColumnName, row[0])
		return nil
	})
	if err != nil {
		return err
	}
	if len(s.baseIndexInfo.ColumnName) == 0 {
		return fmt.Errorf("unknow index %v in %v", s.baseIndex, s.tableName(s.baseDB, s.baseTable))
	}
	return err
}

func (s *splitByBase) getBaseDistinctValues(engine *db.Engine) error {
	startTime := time.Now()
	idxCols := strings.Join(s.baseIndexInfo.ColumnName, ",")
	query := fmt.Sprintf("select distinct %s from %s order by %s",
		idxCols, s.tableName(s.baseDB, s.baseTable), idxCols)
	rows, err := queryAllRows(engine.DB, query)
	if err != nil {
		return err
	}
	s.distinctValues = rows
	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get base table distinct values", endTime.Sub(startTime).String()),
	)

	return nil
}

func (s *splitByBase) calculateRegionNum(engine *db.Engine, totalWriteRows int) (int, error) {
	startTime := time.Now()
	baseRows, err := s.getBaseTableCount(engine)
	if err != nil {
		return 0, err
	}
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("get base table record counts", endTime.Sub(startTime).String()),
	)

	startTime = time.Now()
	baseIndexRegions, err := s.getBaseTableIndexRegionCount(engine)
	if err != nil {
		return 0, err
	}
	if baseIndexRegions < 1 {
		baseIndexRegions = 1
	}
	capacity := baseRows / baseIndexRegions
	// To prevent too little data stored in the region, generally a region can store 10,000 indexes,
	// Because too many regions are not good
	if capacity < 10000 {
		capacity = 10000
	}

	//count := totalWriteRows / capacity
	count := math.Ceil(float64(totalWriteRows) / float64(capacity))
	if count < 1 {
		count = 1
	}
	endTime = time.Now()
	zlog.Logger.Info("Run task info",
		zap.Int("get base table index regions", baseIndexRegions),
		zap.Int("sampling split table index regions", int(count)),
		zap.String("cost time", endTime.Sub(startTime).String()),
	)

	return int(count), nil
}

func (s *splitByBase) getBaseTableCount(engine *db.Engine) (int, error) {
	count := 0
	query := fmt.Sprintf("select count(1) from %v", s.tableName(s.baseDB, s.baseTable))
	err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) != 1 {
			panic("result row is not row counts, should never happen")
		}
		v, err := strconv.Atoi(row[0])
		if err != nil {
			return err
		}
		count = v
		return nil
	})
	return count, err
}

func (s *splitByBase) getBaseTableIndexRegionCount(engine *db.Engine) (int, error) {
	count := 0
	query := fmt.Sprintf("show table %s index %s regions", s.tableName(s.baseDB, s.baseTable), s.baseIndex)
	err := queryRows(engine.DB, query, func(row, cols []string) error {
		count++
		return nil
	})
	return count, err
}

func (s *splitByBase) tableName(db, table string) string {
	return fmt.Sprintf("%s.%s", db, table)
}

func queryAllRows(Engine *sql.DB, SQL string) ([][]string, error) {
	rows, err := Engine.Query(SQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, err
	}

	cols, err1 := rows.Columns()
	if err1 != nil {
		return nil, err1
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
			return nil, err1
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
		return nil, err
	}
	return actualRows, nil
}
