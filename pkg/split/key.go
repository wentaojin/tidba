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

func IncludeTableSplitKey(dbName string, statusAddr string, concurrency int, includeTables []string, outDir string, engine *db.Engine) error {
	var tasks []*Task

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db [%s] table '%v' not exist", dbName, notExistTables)
	}

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}

func FilterTableSplitKey(dbName string, statusAddr string, concurrency int, excludeTables []string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAllTables(allTables, excludeTables)

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)
	return nil

}

func RegexpTableSplitKey(dbName string, statusAddr string, concurrency int, regex string, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	includeTables := util.RegexpFromAllTables(allTables, regex)

	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}

	var tasks []*Task
	for i, table := range includeTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}

func AllTableSplitKey(dbName string, statusAddr string, concurrency int, outDir string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}
	loc, err := getSessionLocation(engine)
	if err != nil {
		return fmt.Errorf("db [%s] get time location failed: %v", dbName, err)
	}
	var tasks []*Task
	for i, table := range allTables {
		tasks = append(tasks, &Task{
			TaskID:     i,
			DbName:     dbName,
			TableName:  table,
			OutDir:     outDir,
			Engine:     engine,
			Loc:        loc,
			StatusAddr: statusAddr,
		})
	}

	taskConcurrencyRunKey(tasks, concurrency, runSplitTableKey)

	return nil
}
