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
package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/pretty"
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/util"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Region struct {
	Count   int            `json:"count"`
	Regions []SingleRegion `json:"regions"`
}

type SingleRegion struct {
	ID       int    `json:"id"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
	Epoch    struct {
		ConfVer int `json:"conf_ver"`
		Version int `json:"version"`
	} `json:"epoch"`
	Peers []struct {
		ID       int    `json:"id"`
		StoreID  int    `json:"store_id"`
		RoleName string `json:"role_name"`
	} `json:"peers"`
	Leader struct {
		ID       int    `json:"id"`
		StoreID  int    `json:"store_id"`
		RoleName string `json:"role_name"`
	} `json:"leader"`
	WrittenBytes    int      `json:"written_bytes"`
	ReadBytes       int      `json:"read_bytes"`
	WrittenKeys     int      `json:"written_keys"`
	ReadKeys        int      `json:"read_keys"`
	ApproximateSize int      `json:"approximate_size"`
	ApproximateKeys int      `json:"approximate_keys"`
	DBINFO          []DBINFO `json:"db_info"`
}

type DBINFO struct {
	RegionID  string `json:"region_id"`
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
}

type ConfigReplica struct {
	MaxReplicas               int    `json:"max-replicas"`
	LocationLabels            string `json:"location-labels"`
	StrictlyMatchLabel        string `json:"strictly-match-label"`
	EnablePlacementRules      string `json:"enable-placement-rules"`
	EnablePlacementRulesCache string `json:"enable-placement-rules-cache"`
	IsolationLevel            string `json:"isolation-level"`
}

type Store struct {
	Count  int `json:"count"`
	Stores []struct {
		Store struct {
			ID             int    `json:"id"`
			Address        string `json:"address"`
			Version        string `json:"version"`
			StatusAddress  string `json:"status_address"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int    `json:"start_timestamp"`
			DeployPath     string `json:"deploy_path"`
			LastHeartbeat  int64  `json:"last_heartbeat"`
			StateName      string `json:"state_name"`
		} `json:"store"`
		Status struct {
			Capacity        string    `json:"capacity"`
			Available       string    `json:"available"`
			UsedSize        string    `json:"used_size"`
			LeaderCount     int       `json:"leader_count"`
			LeaderWeight    int       `json:"leader_weight"`
			LeaderScore     int       `json:"leader_score"`
			LeaderSize      int       `json:"leader_size"`
			RegionCount     int       `json:"region_count"`
			RegionWeight    int       `json:"region_weight"`
			RegionScore     int       `json:"region_score"`
			RegionSize      int       `json:"region_size"`
			SlowScore       int       `json:"slow_score"`
			StartTs         time.Time `json:"start_ts"`
			LastHeartbeatTs time.Time `json:"last_heartbeat_ts"`
			Uptime          string    `json:"uptime"`
		} `json:"status"`
	} `json:"stores"`
}

func GetCurrentRegionPeer(peers string, regionType, pdAddr string, engine *db.Engine) error {
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}

	var (
		data     [][]string
		regionID []string
	)
	tmpRegionMap := make(map[string][]string)

	fmt.Printf(">--------+  Region Count: %v  +--------<\n", region.Count)
	fmt.Printf(">--------+  Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)

	columns := []string{"RegionID", "LeaderID", "Peers", "ApproximateSize", "ApproximateKeys", "Category"}

	for _, r := range region.Regions {
		// 获取指定副本数 Region
		peerInt, err := strconv.Atoi(peers)
		if err != nil {
			return err
		}
		if len(r.Peers) == peerInt {
			regionID = append(regionID, strconv.Itoa(r.ID))
			var tmpData []string
			peerByte, err := json.Marshal(r.Peers)
			if err != nil {
				return err
			}
			tmpData = append(tmpData, strconv.Itoa(r.ID), strconv.Itoa(r.Leader.ID), string(peerByte), strconv.Itoa(r.ApproximateSize), strconv.Itoa(r.ApproximateKeys))
			tmpRegionMap[strconv.Itoa(r.ID)] = tmpData
		}
	}

	// 获取 region 库表/索引信息
	if len(regionID) == 0 {
		fmt.Printf(">--------+  Replica Peer [%s] Isn't Exist +--------<\n", peers)
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionID)
	if err != nil {
		return err
	}

	for k, v := range regionMap {
		// region ID Map
		if _, ok := tmpRegionMap[k]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range v {
				var dbInfo DBINFO
				if err := json.Unmarshal([]byte(m), &dbInfo); err != nil {
					return err
				}
				dbInfos = append(dbInfos, dbInfo)
			}

			dbInfoByte, err := json.Marshal(dbInfos)
			if err != nil {
				return err
			}
			tmpData = append(tmpRegionMap[k], string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			return fmt.Errorf("region id [%s] isn't exist", v)
		}
	}

	util.QueryResultFormatTableWithRowLineStyle(columns, data)
	return nil
}

func GetMajorDownRegionPeer(regionType, pdAddr string, downTiKVS []string, engine *db.Engine) error {
	var (
		data        [][]string
		regionID    []string
		downStores  []int
		panicStores []string
	)

	region, err := getClusterDownRegion(pdAddr)
	if err != nil {
		return err
	}
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	stores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}

	for _, t := range downTiKVS {
		for _, s := range stores.Stores {
			if strings.EqualFold(t, s.Store.Address) && strings.EqualFold(s.Store.StateName, "DOWN") {
				downStores = append(downStores, s.Store.ID)
			} else {
				panicStores = append(panicStores, t)
			}
		}
	}

	if len(panicStores) > 0 {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check", panicStores)
	}

	tmpRegionMap := make(map[string][]string)

	fmt.Printf(">--------+  Region Count: %v  +--------<\n", region.Count)
	fmt.Printf(">--------+  Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	columns := []string{"RegionID", "LeaderID", "Peers", "ApproximateSize", "ApproximateKeys", "Category"}
	for _, r := range region.Regions {
		// 获取异常 Region
		var downRegionArr []int
		for _, s := range r.Peers {
			for _, downStore := range downStores {
				if downStore == s.StoreID {
					downRegionArr = append(downRegionArr, s.ID)
				}
			}
		}

		// 异常副本数大于或等于正常副本数
		if len(downRegionArr) >= (len(r.Peers) - len(downRegionArr)) {
			regionID = append(regionID, strconv.Itoa(r.ID))
			var tmpData []string
			peerByte, err := json.Marshal(r.Peers)
			if err != nil {
				return err
			}
			tmpData = append(tmpData, strconv.Itoa(r.ID), strconv.Itoa(r.Leader.ID), string(peerByte), strconv.Itoa(r.ApproximateSize), strconv.Itoa(r.ApproximateKeys))
			tmpRegionMap[strconv.Itoa(r.ID)] = tmpData
		}
	}

	// 获取 region 库表/索引信息
	if len(regionID) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionID)
	if err != nil {
		return err
	}

	for k, v := range regionMap {
		// region ID Map
		if _, ok := tmpRegionMap[k]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range v {
				var dbInfo DBINFO
				if err := json.Unmarshal([]byte(m), &dbInfo); err != nil {
					return err
				}
				dbInfos = append(dbInfos, dbInfo)
			}

			dbInfoByte, err := json.Marshal(dbInfos)
			if err != nil {
				return err
			}

			tmpData = append(tmpRegionMap[k], string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			return fmt.Errorf("region id [%s] isn't exist", v)
		}
	}

	util.QueryResultFormatTableWithRowLineStyle(columns, data)
	return nil
}

func GetLessDownRegionPeer(regionType, pdAddr string, downTiKVS []string, engine *db.Engine) error {
	var (
		data              [][]string
		regionID          []string
		downStores        []int
		panicStores       []string
		downRegionIDStore []string
	)

	region, err := getClusterDownRegion(pdAddr)
	if err != nil {
		return err
	}
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	stores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}

	for _, t := range downTiKVS {
		for _, s := range stores.Stores {
			if strings.EqualFold(t, s.Store.Address) && strings.EqualFold(s.Store.StateName, "DOWN") {
				downStores = append(downStores, s.Store.ID)
			} else {
				panicStores = append(panicStores, t)
			}
		}
	}

	if len(panicStores) > 0 {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check", panicStores)
	}

	tmpRegionMap := make(map[string][]string)

	fmt.Printf(">--------+  Region Count: %v  +--------<\n", region.Count)
	fmt.Printf(">--------+  Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)

	columns := []string{"RegionID", "LeaderID", "Peers", "ApproximateSize", "ApproximateKeys", "Category"}
	for _, r := range region.Regions {
		// 获取异常 Region
		var downRegionStoreArr []int

		for _, s := range r.Peers {
			for _, downStore := range downStores {
				if downStore == s.StoreID {
					downRegionStoreArr = append(downRegionStoreArr, s.StoreID)
				}
			}
		}

		// 异常副本数小于正常副本数
		if len(downRegionStoreArr) < (len(r.Peers) - len(downRegionStoreArr)) {
			regionID = append(regionID, strconv.Itoa(r.ID))
			var tmpData []string
			peerByte, err := json.Marshal(r.Peers)
			if err != nil {
				return err
			}
			tmpData = append(tmpData, strconv.Itoa(r.ID), strconv.Itoa(r.Leader.ID), string(peerByte), strconv.Itoa(r.ApproximateSize), strconv.Itoa(r.ApproximateKeys))
			tmpRegionMap[strconv.Itoa(r.ID)] = tmpData

			// 修复建议
			for _, downRegionStore := range downRegionStoreArr {
				downRegionIDStore = append(downRegionIDStore, fmt.Sprintf(`operator add remove-peer %d %d`, r.ID, downRegionStore))
			}
		}
	}

	// 获取 region 库表/索引信息
	if len(regionID) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionID)
	if err != nil {
		return err
	}

	for k, v := range regionMap {
		// region ID Map
		if _, ok := tmpRegionMap[k]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range v {
				var dbInfo DBINFO
				if err := json.Unmarshal([]byte(m), &dbInfo); err != nil {
					return err
				}
				dbInfos = append(dbInfos, dbInfo)
			}

			dbInfoByte, err := json.Marshal(dbInfos)
			if err != nil {
				return err
			}

			tmpData = append(tmpRegionMap[k], string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			return fmt.Errorf("region id [%s] isn't exist", v)
		}
	}
	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	// 修复建议
	for _, downRegionStore := range downRegionIDStore {
		fmt.Printf("%s\n", downRegionStore)
	}
	return nil
}

func GetRegionIDINFO(regionID []string, regionType, pdAddr string, engine *db.Engine) error {
	// 获取 region 库表/索引信息
	regionMap, err := engine.GetRegionStatus(regionType, regionID)
	if err != nil {
		return err
	}

	for _, r := range regionID {
		singleRegion, err := getClusterSingleRegion(pdAddr, r)
		if err != nil {
			return err
		}
		if _, ok := regionMap[r]; ok {
			fmt.Printf(">--------+  Region ID: %v  +--------<\n", r)

			var dbInfos []DBINFO

			for _, m := range regionMap[r] {
				var dbInfo DBINFO
				if err := json.Unmarshal([]byte(m), &dbInfo); err != nil {
					return err
				}
				dbInfos = append(dbInfos, dbInfo)
			}

			singleRegion.DBINFO = dbInfos

			jsonByte, err := json.Marshal(singleRegion)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", pretty.Pretty(jsonByte))
		} else {
			return fmt.Errorf("region id [%s] isn't exist", r)
		}
	}
	return nil
}

func getClusterAllRegion(pdAddr string) (Region, error) {
	var region Region

	regionAPI := fmt.Sprintf("http://%s/pd/api/v1/regions", pdAddr)
	response, err := http.Get(regionAPI)
	if err != nil {
		return region, fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return region, fmt.Errorf("read request data failed: %v", err)
	}
	if err := json.Unmarshal(body, &region); err != nil {
		return region, fmt.Errorf("json Unmarshal struct region failed: %v", err)
	}
	return region, nil
}

func getClusterDownRegion(pdAddr string) (Region, error) {
	var region Region

	regionAPI := fmt.Sprintf("http://%s/pd/api/v1/regions/check/down-peer", pdAddr)
	response, err := http.Get(regionAPI)
	if err != nil {
		return region, fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return region, fmt.Errorf("read request data failed: %v", err)
	}
	if err := json.Unmarshal(body, &region); err != nil {
		return region, fmt.Errorf("json Unmarshal struct region failed: %v", err)
	}
	return region, nil
}

func getClusterConfigReplica(pdAddr string) (ConfigReplica, error) {
	var cfgReplica ConfigReplica
	cfgReplicaAPI := fmt.Sprintf("http://%s/pd/api/v1/config/replicate", pdAddr)
	response, err := http.Get(cfgReplicaAPI)
	if err != nil {
		return cfgReplica, fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return cfgReplica, fmt.Errorf("read request data failed: %v", err)
	}
	if err := json.Unmarshal(body, &cfgReplica); err != nil {
		return cfgReplica, fmt.Errorf("json Unmarshal struct region failed: %v", err)
	}
	return cfgReplica, nil
}

func getClusterSingleRegion(pdAddr string, regionID string) (SingleRegion, error) {
	var region SingleRegion

	regionAPI := fmt.Sprintf("http://%s/pd/api/v1/region/id/%s", pdAddr, regionID)
	response, err := http.Get(regionAPI)
	if err != nil {
		return region, fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return region, fmt.Errorf("read request data failed: %v", err)
	}
	if err := json.Unmarshal(body, &region); err != nil {
		return region, fmt.Errorf("json Unmarshal struct region failed: %v", err)
	}
	return region, nil
}

func getClusterStores(pdAddr string) (Store, error) {
	var store Store

	storeAPI := fmt.Sprintf("http://%s/pd/api/v1/stores", pdAddr)
	response, err := http.Get(storeAPI)
	if err != nil {
		return store, fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return store, fmt.Errorf("read request data failed: %v", err)
	}
	if err := json.Unmarshal(body, &store); err != nil {
		return store, fmt.Errorf("json Unmarshal struct region failed: %v", err)
	}
	return store, nil
}
