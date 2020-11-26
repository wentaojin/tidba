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
	"fmt"

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
