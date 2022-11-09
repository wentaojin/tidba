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
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/util"
	"golang.org/x/sync/errgroup"
	"strings"
)

/*
table region data leader view
*/
func IncludeTableRegionDataView(dbName string, concurrency int, includeTables []string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistInclude(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)
	for _, t := range includeTables {
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
			DESC`, dbName, t)
		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func FilterTableRegionDataView(dbName string, concurrency int, excludeTables []string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	includeTables := util.FilterFromAll(allTables, excludeTables)

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)

	g := &errgroup.Group{}
	g.SetLimit(concurrency)
	for _, t := range includeTables {
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
  DESC`, dbName, t)
		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func RegexpTableRegionDataView(dbName string, concurrency int, regex string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	includeTables := util.RegexpFromAll(allTables, regex)

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range includeTables {
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
  DESC`, dbName, t)
		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func AllTableRegionDataView(dbName string, concurrency int, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)

	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range allTables {
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
  DESC`, dbName, t)

		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

/*
table region index leader view
*/
func IncludeTableRegionIndexView(dbName string, concurrency int, includeTables []string, indexNames []string, engine *db.Engine) error {
	if len(includeTables) > 1 {
		return fmt.Errorf("db %s table %v can't exceed one, current value %d", dbName, includeTables, len(includeTables))
	}

	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistInclude(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)
	for _, t := range includeTables {
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
	4 DESC;`, dbName, t, strings.Join(indexes, ","))
		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func AllTableRegionIndexView(dbName string, concurrency int, indexNames []string, engine *db.Engine) error {
	var indexes []string
	for _, i := range indexNames {
		index := fmt.Sprintf("'%s'", strings.ToLower(i))
		indexes = append(indexes, index)
	}

	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range allTables {
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
	4 DESC;`, dbName, t, strings.Join(indexes, ","))
		g.Go(func() error {
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
