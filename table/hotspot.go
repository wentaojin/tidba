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
all table data and index region hotspot view
*/
func AllTableDataAndIndexRegionHotspotView(dbName string, hotType string, limit int, engine *db.Engine) error {

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)

	sql := fmt.Sprintf(`SELECT
	h.TABLE_NAME,
	h.INDEX_NAME,
	h.TABLE_ID,
	h.INDEX_ID,
	r.STORE_ID,
	h.REGION_ID,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS,
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1
	AND UPPER( h.DB_NAME ) = UPPER( '%s' ) 
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, dbName, hotType, limit)

	cols, res, err := engine.Query(sql)
	if err != nil {
		return err
	}
	util.QueryResultFormatTableWithBaseStyle(cols, res)
	return nil
}

/*
table data region hotspot view
*/
func IncludeTableDataRegionHotspotView(dbName string, hotType string, limit, concurrency int, includeTables []string, engine *db.Engine) error {
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
	h.TABLE_NAME,
	h.TABLE_ID,
	r.STORE_ID,
	h.REGION_ID,
	s.IS_INDEX,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS,
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1
	AND s.IS_INDEX = 0
	AND UPPER( h.DB_NAME ) = UPPER( '%s' ) 
	AND UPPER(h.TABLE_NAME) = UPPER('%s')
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, dbName, t, hotType, limit)
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

func FilterTableDataRegionHotspotView(dbName string, hotType string, limit, concurrency int, excludeTables []string, engine *db.Engine) error {
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
	h.TABLE_NAME,
	h.TABLE_ID,
	r.STORE_ID,
	h.REGION_ID,
	s.IS_INDEX,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS,
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1
	AND s.IS_INDEX = 0
	AND UPPER( h.DB_NAME ) = UPPER( '%s' ) 
	AND UPPER(h.TABLE_NAME) = UPPER('%s')
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, dbName, t, hotType, limit)
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

func RegexpTableDataRegionHotspotView(dbName string, hotType string, limit, concurrency int, regexTables string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	includeTables := util.RegexpFromAll(allTables, regexTables)

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range includeTables {
		sql := fmt.Sprintf(`SELECT
	h.TABLE_NAME,
	h.TABLE_ID,
	r.STORE_ID,
	h.REGION_ID,
	s.IS_INDEX,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS,
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1
	AND s.IS_INDEX = 0
	AND UPPER( h.DB_NAME ) = UPPER( '%s' ) 
	AND UPPER(h.TABLE_NAME) = UPPER('%s')
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, dbName, t, hotType, limit)
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
table index region hotspot view
*/
func IncludeTableIndexRegionHotspotView(dbName string, hotType string, limit, concurrency int, includeTables, indexNames []string, engine *db.Engine) error {
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

	tableIndexesMap, err := engine.GetTableAllIndex(dbName, includeTables)
	if err != nil {
		return err
	}

	fmt.Printf(">--------+  DB: %s  +--------<\n", dbName)

	for _, t := range includeTables {
		if _, ok := tableIndexesMap[strings.ToUpper(t)]; ok {
			isSubset, notExistTableIndexes := util.IsExistInclude(tableIndexesMap[strings.ToUpper(t)], indexNames)
			if !isSubset {
				return fmt.Errorf("db %s table %s index '%s' not exist", dbName, t, notExistTableIndexes)
			}

			var indexes []string
			for _, idx := range indexNames {
				indexes = append(indexes, fmt.Sprintf("'%s'", idx))
			}
			sql := fmt.Sprintf(`SELECT
	h.TABLE_NAME,
	h.TABLE_ID,
	r.STORE_ID,
	h.REGION_ID,
	s.IS_INDEX,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS,
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1
	AND s.IS_INDEX = 1
	AND UPPER( h.DB_NAME ) = UPPER( '%s' ) 
	AND UPPER(h.TABLE_NAME) = UPPER('%s')
    AND UPPER( h.INDEX_NAME) IN (%s)
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, dbName, t, strings.Join(indexes, ","), hotType, limit)
			cols, res, err := engine.Query(sql)
			if err != nil {
				return err
			}
			util.QueryResultFormatTableWithBaseStyle(cols, res)
		} else {
			return fmt.Errorf("db %s table '%v' not exist", dbName, t)
		}
	}
	return nil
}
