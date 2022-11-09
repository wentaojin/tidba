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
package store

import (
	"fmt"
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/util"
	"golang.org/x/sync/errgroup"
)

/*
all table data and index store hotspot view
*/
func AllDBDataAndIndexStoreHotspotView(hotType string, limit int, engine *db.Engine) error {
	fmt.Printf(">--------+  DB: ALL DBS  +--------<\n")
	sql := fmt.Sprintf(`SELECT
    h.DB_NAME,
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
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT % d`, hotType, limit)

	cols, res, err := engine.Query(sql)
	if err != nil {
		return err
	}
	util.QueryResultFormatTableWithBaseStyle(cols, res)
	return nil
}

func AllTableDataAndIndexStoreHotspotView(dbName string, hotType string, limit int, engine *db.Engine) error {

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
table store hotspot view
*/
func IncludeStoreRegionHotspotView(hotType string, limit, concurrency int, includeStores []string, engine *db.Engine) error {

	fmt.Printf(">--------+  STORE: %v  +--------<\n", includeStores)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, s := range includeStores {
		sql := fmt.Sprintf(`SELECT
    h.DB_NAME,
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
	AND r.STORE_ID  = %v
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT %d`, s, hotType, limit)
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

func FilterStoreRegionHotspotView(hotType string, limit, concurrency int, excludeTables []string, engine *db.Engine) error {
	allStores, err := engine.GetAllStores()
	if err != nil {
		return err
	}

	includeStores := util.FilterFromAll(allStores, excludeTables)

	fmt.Printf(">--------+  STORE: %v  +--------<\n", includeStores)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, s := range includeStores {
		sql := fmt.Sprintf(`SELECT
    h.DB_NAME,
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
	AND r.STORE_ID  = %v
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT %d`, s, hotType, limit)
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

func RegexpStoreRegionHotspotView(hotType string, limit, concurrency int, regexStores string, engine *db.Engine) error {
	allStores, err := engine.GetAllStores()
	if err != nil {
		return err
	}

	includeStores := util.RegexpFromAll(allStores, regexStores)

	fmt.Printf(">--------+  STORE: %v  +--------<\n", includeStores)
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, s := range includeStores {
		sql := fmt.Sprintf(`SELECT
    h.DB_NAME,
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
	AND r.STORE_ID  = %v
	AND UPPER( h.TYPE ) = UPPER( '%s' ) 
ORDER BY
	FLOW_BYTES DESC 
	LIMIT %d`, s, hotType, limit)
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
