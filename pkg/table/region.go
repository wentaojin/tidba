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
	"strings"
	"sync"

	"github.com/WentaoJin/tidba/pkg/util"
	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
)

/*
	table region data leader view
*/
func IncludeTableRegionDataView(dbName string, concurrency int, includeTables []string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
  table_name,
  store_id,
	count( * ) as leader_counts
FROM
	(
SELECT
	t1.table_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s') 
	AND lower(t1.table_name) = lower('%s') 
	AND t1.is_index = 0 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
  table_name
ORDER BY
	3
  DESC`, dbName, j)
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}

func FilterTableRegionDataView(dbName string, concurrency int, excludeTables []string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAllTables(allTables, excludeTables)

	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
  table_name,
  store_id,
	count( * ) as leader_counts
FROM
	(
SELECT
	t1.table_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s') 
	AND lower(t1.table_name) = lower('%s') 
	AND t1.is_index = 0 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
  table_name
ORDER BY
	3
  DESC`, dbName, j)
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}

func RegexpTableRegionDataView(dbName string, concurrency int, regex string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.RegexpFromAllTables(allTables, regex)
	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
  table_name,
  store_id,
	count( * ) as leader_counts
FROM
	(
SELECT
	t1.table_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s') 
	AND lower(t1.table_name) = lower('%s') 
	AND t1.is_index = 0 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
  table_name
ORDER BY
	3
  DESC`, dbName, j)
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()

	return nil
}

func AllTableRegionDataView(dbName string, concurrency int, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(allTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
  table_name,
  store_id,
	count( * ) as leader_counts
FROM
	(
SELECT
	t1.table_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s') 
	AND lower(t1.table_name) = lower('%s') 
	AND t1.is_index = 0 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
  table_name
ORDER BY
	3
  DESC`, dbName, j)
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)

				//fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range allTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}

/*
	table region index leader view
*/
func IncludeTableRegionIndexView(dbName string, concurrency int, includeTables []string, indexNames []string, engine *db.Engine) error {
	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistIncludeTable(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
	table_name,
	index_name,
	store_id,
	count( * ) AS leader_counts 
FROM
	(
SELECT
	t1.table_name,
	t1.index_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s')
	AND lower(t1.table_name) = lower('%s') 
	AND lower(t1.index_name) in (%s) 
	AND t1.is_index = 1 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
	table_name,
	index_name 
ORDER BY
	4 DESC;`, dbName, j, strings.Join(indexes, ","))
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}

func FilterTableRegionIndexView(dbName string, concurrency int, excludeTables, indexNames []string, engine *db.Engine) error {

	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAllTables(allTables, excludeTables)
	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
	table_name,
	index_name,
	store_id,
	count( * ) AS leader_counts 
FROM
	(
SELECT
	t1.table_name,
	t1.index_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s')
	AND lower(t1.table_name) = lower('%s') 
	AND lower(t1.index_name) in (%s) 
	AND t1.is_index = 1 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
	table_name,
	index_name 
ORDER BY
	4 DESC;`, dbName, j, strings.Join(indexes, ","))
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				//fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}

func RegexpTableRegionIndexView(dbName string, concurrency int, regex string, indexNames []string, engine *db.Engine) error {

	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	includeTables := util.RegexpFromAllTables(allTables, regex)
	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(includeTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
	table_name,
	index_name,
	store_id,
	count( * ) AS leader_counts 
FROM
	(
SELECT
	t1.table_name,
	t1.index_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s')
	AND lower(t1.table_name) = lower('%s') 
	AND lower(t1.index_name) in (%s) 
	AND t1.is_index = 1 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
	table_name,
	index_name 
ORDER BY
	4 DESC;`, dbName, j, strings.Join(indexes, ","))
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)
				//fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range includeTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()

	return nil
}

func AllTableRegionIndexView(dbName string, concurrency int, indexNames []string, engine *db.Engine) error {
	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	allTables, err := db.GetAllTables(dbName, engine)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	jobsChannel := make(chan string, len(allTables))

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	for i := 0; i < concurrency; i++ {
		go func(threadID int) {
			for j := range jobsChannel {
				sql := fmt.Sprintf(`SELECT
	table_name,
	index_name,
	store_id,
	count( * ) AS leader_counts 
FROM
	(
SELECT
	t1.table_name,
	t1.index_name,
	t1.region_id,
	t2.store_id 
FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id 
	AND lower(t1.db_name) = lower('%s')
	AND lower(t1.table_name) = lower('%s') 
	AND lower(t1.index_name) in (%s) 
	AND t1.is_index = 1 
	AND t2.is_leader = 1 
	) t 
GROUP BY
	store_id,
	table_name,
	index_name 
ORDER BY
	4 DESC;`, dbName, j, strings.Join(indexes, ","))
				cols, res, err := engine.QuerySQL(sql)
				if err != nil {
					zlog.Logger.Fatal("Task run sql failed",
						zap.Int("threadID", threadID),
						zap.String("sql", sql),
						zap.Error(err),
					)
				}
				util.QueryResultFormatTableWithBaseStyle(cols, res)

				//fmt.Printf("threadID [%d] task execute success.\n", threadID)
				wg.Done()
			}
		}(i)
	}

	for _, t := range allTables {
		jobsChannel <- t
		wg.Add(1)

	}
	wg.Wait()
	return nil
}
