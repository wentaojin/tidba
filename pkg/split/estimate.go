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
	"fmt"

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
