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
package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/wentaojin/tidba/util"
	"golang.org/x/sync/errgroup"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func NewMySQLEngine(dbUser, dbPassword, ipAddr string, dbPort int, dbName string) (*Engine, error) {
	var (
		dsn string
	)
	if dbName != "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&timeout=120s&parseTime=true", dbUser, dbPassword, ipAddr,
			dbPort, dbName)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&timeout=120s&parseTime=true", dbUser, dbPassword, ipAddr,
			dbPort)
	}
	db, err := sql.Open("mysql", dsn) // this does not really open a new connection
	if err != nil {
		return &Engine{MySQLDB: db}, fmt.Errorf("error on initializing database connection: %v", err)
	}
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(10)

	err = db.Ping() // This DOES open a connection if necessary. This makes sure the database is accessible
	if err != nil {
		return &Engine{MySQLDB: db}, fmt.Errorf("error on opening database connection: %v", err)
	}

	return &Engine{MySQLDB: db}, nil
}

func (e *Engine) IsExistDbName(dbName string) bool {
	querySQL := fmt.Sprintf(`SELECT
	count( schema_name ) AS SCHEMA_NAME 
FROM
	information_schema.SCHEMATA 
WHERE
	lower( schema_name ) = lower( '%s' )`, dbName)
	_, res, _ := e.Query(querySQL)
	if res[0]["SCHEMA_NAME"] == "0" {
		return false
	}
	return true
}

// GetAllTables gets all table name from db
func (e *Engine) GetAllTables(dbName string) ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	table_name 
FROM
	information_schema.TABLES 
WHERE
	lower(table_schema) = lower('%s')`, dbName)

	var tables []string

	_, res, err := e.Query(querySQL)
	if err != nil {
		return tables, fmt.Errorf("failed get all table name from db: %v", err)
	}
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (e *Engine) GetTableAllIndex(dbName string, tableName []string) (map[string][]string, error) {
	querySQL := fmt.Sprintf(`SELECT
    TABLE_NAME,
	INDEX_NAME
FROM
	INFORMATION_SCHEMA.STATISTICS 
WHERE
	UPPER(TABLE_SCHEMA) = UPPER('%s') 
	AND UPPER(TABLE_NAME) IN (%s)`, dbName, tableName)

	tableMap := make(map[string][]string)

	_, res, err := e.Query(querySQL)
	if err != nil {
		return tableMap, fmt.Errorf("failed get all index name from db: %v", err)
	}

	for _, t := range tableName {
		var tableArr []string
		for _, r := range res {
			if strings.EqualFold(r["TABLE_NAME"], t) {
				tableArr = append(tableArr, r["INDEX_NAME"])
			}
		}
		tableMap[strings.ToUpper(t)] = tableArr
	}

	return tableMap, nil
}

func (e *Engine) GetAllStores() ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT DISTINCT STORE_ID FROM INFORMATION_SCHEMA.TIKV_REGION_PEERS`)

	var stores []string

	_, res, err := e.Query(querySQL)
	if err != nil {
		return stores, fmt.Errorf("failed get all store id from cluster: %v", err)
	}

	for _, r := range res {
		stores = append(stores, r["STORE_ID"])
	}

	return stores, nil
}

func (e *Engine) GetRegionStatus(regionType string, regionArr []string, concurrency int) (*sync.Map, error) {
	var querySQL string

	regionMap := &sync.Map{}

	regionSpitArr := util.ArrayStringGroupsOf(regionArr, 1000)

	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	regionIDMapChan := make(chan map[string][]string, concurrency)
	go func() {
		for c := range regionIDMapChan {
			// merge map
			for k, v := range c {
				regionMap.Store(k, v)
			}
		}
	}()

	for _, r := range regionSpitArr {
		regions := r
		g.Go(func() error {
			if strings.EqualFold(regionType, "data") {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 0
    AND REGION_ID IN (%s)`, strings.Join(regions, ","))
			} else if strings.EqualFold(regionType, "index") {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 1
    AND REGION_ID IN (%s)`, strings.Join(regions, ","))
			} else if strings.EqualFold(regionType, "all") {
				querySQL = fmt.Sprintf(`SELECT 
    REGION_ID,
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    REGION_ID IN (%s)`, strings.Join(regions, ","))
			} else {
				return fmt.Errorf("region type [%s] isn't support", regionType)
			}

			_, res, err := e.Query(querySQL)
			if err != nil {
				return fmt.Errorf("failed get all store id from cluster: %v", err)
			}

			tmpRegionIDMap := make(map[string][]string)
			for _, s := range res {
				regionByte, err := json.Marshal(s)
				if err != nil {
					return err
				}
				if _, ok := tmpRegionIDMap[s["REGION_ID"]]; ok {
					var tmpRegionArr []string
					tmpRegionArr = append(tmpRegionArr, tmpRegionIDMap[s["REGION_ID"]]...)
					tmpRegionArr = append(tmpRegionArr, string(regionByte))
					tmpRegionIDMap[s["REGION_ID"]] = tmpRegionArr
				} else {
					tmpRegionIDMap[s["REGION_ID"]] = []string{string(regionByte)}
				}
			}

			regionIDMapChan <- tmpRegionIDMap
			return nil
		})

	}

	if err := g.Wait(); err != nil {
		return regionMap, err
	}

	return regionMap, nil
}

func (e *Engine) GetRegionStatusGroupByTable(regionType string, regionArr []string, concurrency int) ([][]string, error) {
	var querySQL string

	var regionData [][]string

	regionSpitArr := util.ArrayStringGroupsOf(regionArr, 1000)

	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	regionChan := make(chan []string, concurrency)
	go func() {
		for c := range regionChan {
			regionData = append(regionData, c)
		}
	}()

	for _, r := range regionSpitArr {
		regions := r
		g.Go(func() error {
			if strings.EqualFold(regionType, "data") {
				querySQL = fmt.Sprintf(`SELECT 
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	COUNT(1) AS COUNTS
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 0
    AND REGION_ID IN (%s)
GROUP BY DB_NAME,TABLE_NAME,INDEX_NAME`, strings.Join(regions, ","))
			} else if strings.EqualFold(regionType, "index") {
				querySQL = fmt.Sprintf(`SELECT 
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	COUNT(1) AS COUNTS
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    IS_INDEX = 1
    AND REGION_ID IN (%s)
GROUP BY DB_NAME,TABLE_NAME,INDEX_NAME`, strings.Join(regions, ","))
			} else if strings.EqualFold(regionType, "all") {
				querySQL = fmt.Sprintf(`SELECT 
	IFNULL(DB_NAME,"NULLABLE") AS DB_NAME,
	IFNULL(TABLE_NAME,"NULLABLE") AS TABLE_NAME,
	IFNULL(INDEX_NAME,"NULLABLE") AS INDEX_NAME,
	COUNT(1) AS COUNTS
FROM
	INFORMATION_SCHEMA.TIKV_REGION_STATUS
WHERE
    REGION_ID IN (%s)
GROUP BY DB_NAME,TABLE_NAME,INDEX_NAME`, strings.Join(regions, ","))
			} else {
				return fmt.Errorf("region type [%s] isn't support", regionType)
			}

			_, res, err := e.Query(querySQL)
			if err != nil {
				return fmt.Errorf("failed get all store id from cluster: %v", err)
			}

			var tmpRegion []string
			for _, s := range res {
				tmpRegion = append(tmpRegion, s["DB_NAME"], s["TABLE_NAME"], s["INDEX_NAME"], s["COUNTS"])
			}

			regionChan <- tmpRegion
			return nil
		})

	}

	if err := g.Wait(); err != nil {
		return regionData, err
	}

	return regionData, nil
}
