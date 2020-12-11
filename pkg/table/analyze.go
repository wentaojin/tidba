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
package table

import (
	"fmt"

	"github.com/WentaoJin/tidba/pkg/util"

	"github.com/WentaoJin/tidba/pkg/db"
)

func IncludeTableAnalyze(dbName string, concurrency int, includeTables []string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistInclude(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	var tasks []Task
	for i, t := range includeTables {
		tasks = append(tasks, Task{
			TaskID: i,
			Engine: engine,
			SQL:    fmt.Sprintf("analyze table %s.%s", dbName, t),
			Err:    nil,
			Fn: func(db *db.Engine, sql string) error {
				if err := tableAnalyze(db, sql); err != nil {
					return err
				}
				return nil
			},
		},
		)
	}

	// concurrency analyze
	concurrencyAnalyzeTable(concurrency, tasks)

	return nil
}

func FilterTableAnalyze(dbName string, concurrency int, excludeTables []string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	var tasks []Task
	for i, t := range util.FilterFromAll(allTables, excludeTables) {
		tasks = append(tasks, Task{
			TaskID: i,
			SQL:    fmt.Sprintf("analyze table %s.%s", dbName, t),
			Engine: engine,
			Err:    nil,
			Fn: func(db *db.Engine, sql string) error {
				if err := tableAnalyze(db, sql); err != nil {
					return err
				}
				return nil
			},
		},
		)
	}

	// concurrency analyze
	concurrencyAnalyzeTable(concurrency, tasks)
	return nil

}

func RegexpTableAnalyze(dbName string, concurrency int, regex string, engine *db.Engine) error {
	var tasks []Task
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	for i, t := range util.RegexpFromAll(allTables, regex) {
		tasks = append(tasks, Task{
			TaskID: i,
			Engine: engine,
			SQL:    fmt.Sprintf("analyze table %s.%s", dbName, t),
			Err:    nil,
			Fn: func(db *db.Engine, sql string) error {
				if err := tableAnalyze(db, sql); err != nil {
					return err
				}
				return nil
			},
		},
		)
	}

	// concurrency analyze
	concurrencyAnalyzeTable(concurrency, tasks)
	return nil
}

func AllTableAnalyze(dbName string, concurrency int, engine *db.Engine) error {
	var tasks []Task
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	for i, t := range allTables {
		tasks = append(tasks, Task{
			TaskID: i,
			Engine: engine,
			SQL:    fmt.Sprintf("analyze table %s.%s", dbName, t),
			Err:    nil,
			Fn: func(db *db.Engine, sql string) error {
				if err := tableAnalyze(db, sql); err != nil {
					return err
				}
				return nil
			},
		},
		)
	}

	// concurrency analyze
	concurrencyAnalyzeTable(concurrency, tasks)
	return nil
}

// concurrencyAnalyzeTable
func concurrencyAnalyzeTable(concurrency int, tasks []Task) {
	pool := NewWorkerPool(tasks, concurrency)
	pool.Start()
}

// tableAnalyze is used to analyze table
func tableAnalyze(db *db.Engine, sql string) (err error) {
	_, err = db.DB.Exec(fmt.Sprintf("%s", sql))
	if err != nil {
		return err
	}
	return nil
}
