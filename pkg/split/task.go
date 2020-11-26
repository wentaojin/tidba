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
	"encoding/json"
	"sync"
	"time"

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
)

// Split table info
type TableInfo struct {
	DbName     string
	TableName  string
	Indexes    []IndexInfo
	HandleName string
	SQL        []SqlInfo
	OutDir     string
	CostTime   string
}

type SqlInfo struct {
	SQL string
}

type IndexInfo struct {
	IndexName  string
	ColumnName []string
}

// Split task
type Task struct {
	TaskID     int
	DbName     string
	TableName  string
	OutDir     string
	Engine     *db.Engine
	Loc        *time.Location
	StatusAddr string
}

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
