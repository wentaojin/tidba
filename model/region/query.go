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
package region

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/pretty"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/logger"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"golang.org/x/sync/errgroup"
)

func GenerateHotspotRegionQuerySql(clusterName, database string, storeAddrs []string, tables []string, indexes []string, hottype string, top int) ([]string, error) {
	var stores []string
	// filter stores
	if len(storeAddrs) > 0 {
		topo, err := operator.GetDeployedClusterTopology(clusterName)
		if err != nil {
			return nil, err
		}
		pdInsts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNamePD)
		if err != nil {
			return nil, err
		}
		allStores, err := getClusterStores(fmt.Sprintf("%s:%d", pdInsts[0].Host, pdInsts[0].Port))
		if err != nil {
			return nil, err
		}
		for _, t := range storeAddrs {
			for _, s := range allStores.Stores {
				if strings.EqualFold(t, s.Store.Address) {
					stores = append(stores, strconv.Itoa(s.Store.ID))
				}
			}
		}
	}

	var bs strings.Builder
	bs.WriteString(`SELECT
	h.TABLE_NAME,
	h.INDEX_NAME,
	h.TABLE_ID,
	h.INDEX_ID,
	r.STORE_ID,
	h.REGION_ID,
	h.MAX_HOT_DEGREE,
	h.FLOW_BYTES,
	s.APPROXIMATE_SIZE,
	s.APPROXIMATE_KEYS
FROM
	INFORMATION_SCHEMA.TIDB_HOT_REGIONS h,
	INFORMATION_SCHEMA.TIKV_REGION_PEERS r,
	INFORMATION_SCHEMA.TIKV_REGION_STATUS s
WHERE
	h.REGION_ID = r.REGION_ID
	AND h.REGION_ID = s.REGION_ID
	AND r.REGION_ID = s.REGION_ID
	AND r.IS_LEADER = 1` + "\n")

	if !strings.EqualFold(database, "") {
		bs.WriteString(fmt.Sprintf("AND h.DB_NAME = '%s'\n", database))
	}

	if len(stores) > 0 {
		bs.WriteString(fmt.Sprintf("AND r.STORE_ID IN (%s)\n", strings.Join(stores, ",")))
	}

	if len(indexes) > 0 {
		if len(tables) == 0 || len(tables) > 1 {
			return nil, fmt.Errorf("when the --index flag is specified, --tables must be specified and only one table name can be configured")
		}
		var inds []string
		for _, ind := range indexes {
			inds = append(inds, fmt.Sprintf("'%s'", ind))
		}
		bs.WriteString(fmt.Sprintf("AND h.INDEX_NAME IN (%s)\n", strings.Join(inds, ",")))
	}

	if strings.EqualFold(hottype, "write") || strings.EqualFold(hottype, "read") {
		bs.WriteString(fmt.Sprintf("AND h.TYPE = '%s'\n", hottype))
	}

	if len(tables) > 0 {
		var queries []string
		for _, t := range tables {
			var newBs strings.Builder
			newBs.WriteString(fmt.Sprintf("%s AND h.TABLE_NAME = '%s'\n", bs.String(), t))
			if top > 0 {
				newBs.WriteString(fmt.Sprintf("ORDER BY FLOW_BYTES DESC LIMIT %d", top))
			} else {
				newBs.WriteString("ORDER BY FLOW_BYTES DESC")
			}
			queries = append(queries, newBs.String())
		}
		return queries, nil
	}
	return []string{bs.String()}, nil
}

func GenerateLeaderDistributedQuerySql(clusterName, database string, storeAddrs []string, tables []string, indexes []string) ([]string, error) {
	var stores []string
	// filter stores
	if len(storeAddrs) > 0 {
		topo, err := operator.GetDeployedClusterTopology(clusterName)
		if err != nil {
			return nil, err
		}
		pdInsts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNamePD)
		if err != nil {
			return nil, err
		}
		allStores, err := getClusterStores(fmt.Sprintf("%s:%d", pdInsts[0].Host, pdInsts[0].Port))
		if err != nil {
			return nil, err
		}
		for _, t := range storeAddrs {
			for _, s := range allStores.Stores {
				if strings.EqualFold(t, s.Store.Address) {
					stores = append(stores, strconv.Itoa(s.Store.ID))
				}
			}
		}
	}

	indLen := len(indexes)
	if indLen > 0 {
		if len(tables) == 0 || len(tables) > 1 {
			return nil, fmt.Errorf("when the --index flag is specified, --tables must be specified and only one table name can be configured")
		}
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("to prevent the database query results from being too large, flag --tables cannot be empty")
	}

	var bs strings.Builder
	bs.WriteString(`SELECT
  table_name,` + "\n")

	if indLen > 0 {
		bs.WriteString(`index_name,` + "\n")
	}
	bs.WriteString(`store_id,` + "\n")
	bs.WriteString(`count(*) as leader_counts
	FROM (
SELECT
	t1.table_name,` + "\n")

	if indLen > 0 {
		bs.WriteString(`t1.index_name,` + "\n")
	}

	bs.WriteString(`t1.region_id,
	t2.store_id
	FROM
	information_schema.TIKV_REGION_STATUS t1,
	information_schema.TIKV_REGION_PEERS t2 
WHERE
	t1.region_id = t2.region_id` + "\n")

	if !strings.EqualFold(database, "") {
		bs.WriteString(fmt.Sprintf("AND t1.db_name = '%s'\n", database))
	}
	if indLen > 0 {
		var inds []string
		for _, ind := range indexes {
			inds = append(inds, fmt.Sprintf("'%s'", ind))
		}
		bs.WriteString(fmt.Sprintf("AND t1.index_name IN (%s)\n", strings.Join(inds, ",")))
		bs.WriteString(`AND t1.is_index = 1` + "\n")
	} else {
		bs.WriteString(`AND t1.is_index = 0` + "\n")
	}
	bs.WriteString(`AND t2.is_leader = 1` + "\n")

	if len(stores) > 0 {
		bs.WriteString(fmt.Sprintf("AND t2.store_id IN (%s)", strings.Join(stores, ",")))
	}

	if len(tables) > 0 {
		var queries []string
		for _, t := range tables {
			var newBs strings.Builder
			newBs.WriteString(fmt.Sprintf("%s AND t1.table_name = '%s'\n", bs.String(), t))
			if indLen > 0 {
				newBs.WriteString(` ) t GROUP BY store_id,table_name,index_name ORDER BY 4 DESC`)
			} else {
				newBs.WriteString(` ) t GROUP BY store_id,table_name ORDER BY 3 DESC`)
			}
			queries = append(queries, newBs.String())
		}
		return queries, nil
	}

	if indLen > 0 {
		bs.WriteString(` ) t GROUP BY store_id,table_name,index_name ORDER BY 4 DESC`)
	} else {
		bs.WriteString(` ) t GROUP BY store_id,table_name ORDER BY 3 DESC`)
	}
	return []string{bs.String()}, nil
}

type MajorityResp struct {
	ReplicaCounts             int
	RegionCounts              int
	DownRegionQueryCounts     int
	DownRegionEffectiveCounts int

	TableHeader      []string
	TableRows        [][]interface{}
	DownRegionPanics []string
}

func QueryMarjorDownRegionPeers(ctx context.Context, clusterName string, db *mysql.Database, pdAddrs string, regionType string, concurrency int, storeAddrs []string) (*MajorityResp, error) {
	startTime := time.Now()
	queryTime := time.Now()

	var pdAddr string
	if pdAddrs != "" {
		pdAddr = pdAddrs
	} else {
		topo, err := operator.GetDeployedClusterTopology(clusterName)
		if err != nil {
			return nil, err
		}
		pdInsts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNamePD)
		if err != nil {
			return nil, err
		}
		pdAddr = fmt.Sprintf("%s:%d", pdInsts[0].Host, pdInsts[0].Port)
	}

	regions, err := getClusterRegions(pdAddr)
	if err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("[Region] query cluster regions info in finished %fs", time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("[Region] query cluster config replica info in finished %fs", time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	allStores, err := getClusterStores(pdAddr)
	if err != nil {
		return nil, err
	}

	downStores := make(map[int]struct{})

	// filter stores
	if len(storeAddrs) > 0 {
		for _, t := range storeAddrs {
			for _, s := range allStores.Stores {
				if strings.EqualFold(t, s.Store.Address) && strings.EqualFold(s.Store.StateName, "DOWN") {
					downStores[s.Store.ID] = struct{}{}
				}
			}
		}
	} else {
		for _, s := range allStores.Stores {
			if strings.EqualFold(s.Store.StateName, "DOWN") {
				downStores[s.Store.ID] = struct{}{}
			}
		}
	}

	logger.Info(fmt.Sprintf("[Region] query cluster down store info in finished %fs", time.Since(queryTime).Seconds()))

	queryTime = time.Now()
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	downRegionsMap := &sync.Map{}

	for _, reg := range regions.Regions {
		r := reg
		g.Go(func() error {
			// down regions
			var downRegions []int

			for _, ds := range r.Peers {
				if _, ok := downStores[ds.ID]; ok {
					downRegions = append(downRegions, r.ID)
				}
			}

			// The number of abnormal copies is greater than or equal to the number of normal copies
			if len(downRegions) >= (len(r.Peers) - len(downRegions)) {
				var tmps []string
				resp := &Response{}

				leaderByte, err := json.Marshal(r.Leader)
				if err != nil {
					return err
				}
				peerByte, err := json.Marshal(r.Peers)
				if err != nil {
					return err
				}
				tmps = append(tmps, strconv.Itoa(r.ID), string(pretty.Pretty(leaderByte)), string(pretty.Pretty(peerByte)))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmps

				downRegionsMap.Store(resp.RegionID, resp.RegionInfo)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, err
	}

	logger.Info(fmt.Sprintf("[Region] query cluster major down peers info in finished %fs", time.Since(queryTime).Seconds()))

	var downRegionIds []string
	length := 0
	downRegionsMap.Range(func(key, value interface{}) bool {
		length++
		downRegionIds = append(downRegionIds, key.(string))
		return true // 继续遍历
	})

	if length == 0 {
		logger.Info(fmt.Sprintf("[Region] query cluster major down peers not found in finished in %fs", time.Since(startTime).Seconds()))
		logger.Info("[Region] the cluster region replica peers normal, not found major down peers, please ignore and skip")
		return &MajorityResp{}, nil
	}

	// query region with database table and index information
	queryTime = time.Now()
	downRegionDbInfoMap, err := db.GetRegionStatus(ctx, regionType, downRegionIds, concurrency)
	if err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("[Region] query cluster major down peers database information in finished %fs", time.Since(queryTime).Seconds()))

	// Ignore Region information that has been deleted but not GCed (regions with empty query results in the tidb interface)
	var (
		data         [][]interface{}
		regionPanics []string
	)

	queryTime = time.Now()

	downRegionDbInfoMap.Range(func(key, value any) bool {
		kRegion := key
		vRegion := value
		if vs, ok := downRegionsMap.Load(kRegion); ok {
			var (
				dbInfos []*DbINFO
				tmpData []interface{}
			)
			for _, m := range vRegion.([]string) {
				var dbInfo *DbINFO
				if err = json.Unmarshal([]byte(m), &dbInfo); err != nil {
					panic(err)
				}
				dbInfos = append(dbInfos, dbInfo)
			}

			dbInfoByte, err := json.Marshal(dbInfos)
			if err != nil {
				panic(err)
			}

			tmpData = append(tmpData, vs.([]interface{})...)
			tmpData = append(tmpData, string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			regionPanics = append(regionPanics, kRegion.(string))
		}
		return true
	})

	logger.Info(fmt.Sprintf("[Region] generate cluster major down peers database information in finished %fs", time.Since(queryTime).Seconds()))

	logger.Info(fmt.Sprintf("[Region] generate cluster major down peers informations in completed %fs", time.Since(startTime).Seconds()))

	return &MajorityResp{
		ReplicaCounts:             cfgReplica.MaxReplicas,
		RegionCounts:              regions.Count,
		DownRegionQueryCounts:     length,
		DownRegionEffectiveCounts: len(data),
		TableHeader:               []string{"RegionID", "Leader", "Peers", "Category"},
		TableRows:                 data,
		DownRegionPanics:          regionPanics,
	}, nil
}

func QueryRegionIDInformation(ctx context.Context, clusterName string, db *mysql.Database, regionID []string, pdAddrs string, concurrency int) ([]*SingleRegion, error) {
	var regions []*SingleRegion
	var pdAddr string
	if pdAddrs != "" {
		pdAddr = pdAddrs
	} else {
		topo, err := operator.GetDeployedClusterTopology(clusterName)
		if err != nil {
			return nil, err
		}
		pdInsts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNamePD)
		if err != nil {
			return nil, err
		}
		pdAddr = fmt.Sprintf("%s:%d", pdInsts[0].Host, pdInsts[0].Port)
	}

	regionMap, err := db.GetRegionStatus(ctx, "all", regionID, concurrency)
	if err != nil {
		return regions, err
	}

	for _, r := range regionID {
		var singleRegion *SingleRegion
		singleRegion, err = getClusterSingleRegion(pdAddr, r)
		if err != nil {
			return regions, err
		}
		if value, ok := regionMap.Load(r); ok {
			if value != nil {
				var dbInfos []*DbINFO
				for _, v := range value.([]string) {
					var dbInfo *DbINFO
					if err = json.Unmarshal([]byte(v), &dbInfo); err != nil {
						return nil, err
					}
					dbInfos = append(dbInfos, dbInfo)

					singleRegion.DbINFO = dbInfos

					regions = append(regions, singleRegion)
				}
			}

			continue
		}
		return regions, fmt.Errorf("the region id [%s] not found", r)
	}

	return regions, nil
}
