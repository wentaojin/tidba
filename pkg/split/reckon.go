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
	"bytes"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"
)

func GenerateSplitByReckonBaseTable(engine *db.Engine, baseDB, baseTable, baseIndex, newDB, newTable, newIndex, outDir string, totalWriteRows int) error {
	// 1. get distinct value.
	s := &splitByBase{
		baseDB:      baseDB,
		baseTable:   baseTable,
		baseIndex:   baseIndex,
		newDB:       newDB,
		newTable:    newTable,
		newIndex:    newIndex,
		outFilePath: path.Join(outDir, fmt.Sprintf("split_%s_%s_by_reckon.sql", baseTable, baseIndex)),
	}

	startTime := time.Now()
	err := s.init(engine)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var err1, err2 error
	var baseTableDistinctCount, regionCount int

	wg.Add(2)
	go func() {
		defer wg.Done()
		baseTableDistinctCount, err1 = s.getBaseTableDistinctCounts(engine)
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

	// generate split sql
	err = s.generateReckonSplit(engine, baseTableDistinctCount, regionCount)
	if err != nil {
		return err
	}

	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("generate split table sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

func (s *splitByBase) getBaseTableDistinctCounts(engine *db.Engine) (int, error) {
	startTime := time.Now()
	idxCols := strings.Join(s.baseIndexInfo.ColumnName, ",")
	query := fmt.Sprintf("select count(distinct %s) from %s order by %s",
		idxCols, s.tableName(s.baseDB, s.baseTable), idxCols)
	count := 0
	if err := queryRows(engine.DB, query, func(row, cols []string) error {
		if len(row) == 0 {
			return fmt.Errorf(fmt.Sprintf("get base table [%s] distinct counts falied", s.baseTable))
		}
		v, err := strconv.Atoi(row[0])
		if err != nil {
			return err
		}
		count = v
		return nil
	}); err != nil {
		return count, err
	}
	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get base table distinct counts", endTime.Sub(startTime).String()),
	)

	return count, nil
}

func (s *splitByBase) generateReckonSplit(engine *db.Engine, baseTableDistinctCount, regionCount int) error {
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

	// calculate region pagination record counts
	startTime := time.Now()
	step := math.Ceil(float64(baseTableDistinctCount) / float64(regionCount))
	if step < 1 {
		step = 1
	}

	// get region segmentation boundary value by paginate sql
	idxCols := strings.Join(s.baseIndexInfo.ColumnName, ",")
	query := fmt.Sprintf(`SELECT
	%[1]s 
FROM
	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT DISTINCT %[1]s FROM %[2]s ) AS S ) AS T 
WHERE
	RowNumber %% %[3]d = 1`, idxCols, s.tableName(s.baseDB, s.baseTable), int(step))

	rows, err := queryAllRows(engine.DB, query)
	if err != nil {
		return err
	}
	s.distinctValues = rows
	endTime := time.Now()
	// log record
	zlog.Logger.Info("Run task info",
		zap.String("get split region demarcation point by paginate sql cost time", endTime.Sub(startTime).String()),
	)

	for i, values := range s.distinctValues {
		if i > 0 {
			sqlBuf.WriteString(",")
		}
		sqlBuf.WriteString("(")
		sqlBuf.WriteString(strings.Join(values, ","))
		sqlBuf.WriteString(")")
	}
	_, err = s.fileWriter.WriteString(sqlBuf.String() + ";\n\n")
	return err
}
