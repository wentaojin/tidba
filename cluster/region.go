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
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"strconv"
	"strings"
)

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

func GetMajorDownRegionPeerByDownTiKVS(regionType, pdAddr string, concurrency int, downTiKVS []string, engine *db.Engine) error {
	var (
		data       [][]string
		downStores []int
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
			}
		}
	}

	if len(downStores) != len(downTiKVS) {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check down store [%v]", downTiKVS, downStores)
	}

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)
	fmt.Println(">--------+  Scan Doing: Please Waiting  +--------<")

	columns := []string{"RegionID", "Leader", "Peers", "Category"}

	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		respArr   []Resp
		regionIDS []string
	)
	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			respArr = append(respArr, c)
		}
	}()

	for _, region := range region.Regions {
		r := region
		g.Go(func() error {
			// 获取异常 Region
			var downRegionArr []int

			for _, ds := range r.DownPeers {
				for _, downStore := range downStores {
					if downStore == ds.Peer.StoreID {
						downRegionArr = append(downRegionArr, r.ID)
					}
				}
			}

			// 异常副本数大于或等于正常副本数
			if len(downRegionArr) >= (len(r.Peers) - len(downRegionArr)) {
				var (
					tmpData []string
					resp    Resp
				)

				leaderByte, err := json.Marshal(r.Leader)
				if err != nil {
					return err
				}
				peerByte, err := json.Marshal(r.Peers)
				if err != nil {
					return err
				}
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(leaderByte), string(peerByte))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmpData

				respChan <- resp
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionIDS)
	if err != nil {
		return err
	}

	// region ID Map
	var (
		regionPanics []string
	)
	for _, resp := range respArr {
		if _, ok := regionMap[resp.RegionID]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range regionMap[resp.RegionID] {
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

			tmpData = append(resp.RegionInfo, string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			regionPanics = append(regionPanics, resp.RegionID)
		}
	}

	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}
	return nil
}

func GetMajorDownRegionPeer(regionType, pdAddr string, concurrency int, engine *db.Engine) error {
	var (
		data [][]string
	)

	region, err := getClusterDownRegion(pdAddr)
	if err != nil {
		return err
	}
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)
	fmt.Println(">--------+  Scan Doing: Please Waiting  +--------<")

	columns := []string{"RegionID", "Leader", "Peers", "Category"}

	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		respArr   []Resp
		regionIDS []string
	)
	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			respArr = append(respArr, c)
		}
	}()

	for _, region := range region.Regions {
		r := region
		g.Go(func() error {
			// 获取异常 Region
			// 异常副本数大于或等于正常副本数
			if len(r.DownPeers) >= (len(r.Peers) - len(r.DownPeers)) {
				var (
					tmpData []string
					resp    Resp
				)

				leaderByte, err := json.Marshal(r.Leader)
				if err != nil {
					return err
				}
				peerByte, err := json.Marshal(r.Peers)
				if err != nil {
					return err
				}
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(leaderByte), string(peerByte))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmpData

				respChan <- resp
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionIDS)
	if err != nil {
		return err
	}

	// region ID Map
	var (
		regionPanics []string
	)
	for _, resp := range respArr {
		if _, ok := regionMap[resp.RegionID]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range regionMap[resp.RegionID] {
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

			tmpData = append(resp.RegionInfo, string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			regionPanics = append(regionPanics, resp.RegionID)
		}
	}

	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}
	return nil
}

func GetLessDownRegionPeerByDownTiKVS(regionType, pdAddr string, concurrency int, downTiKVS []string, engine *db.Engine) error {
	var (
		data       [][]string
		downStores []int
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
			}
		}
	}

	if len(downStores) != len(downTiKVS) {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check down store [%v]", downTiKVS, downStores)
	}

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)
	fmt.Println(">--------+  Scan Doing: Please Waiting  +--------<")

	columns := []string{"RegionID", "Leader", "Peers", "Category"}

	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		respArr   []Resp
		regionIDS []string
	)
	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			respArr = append(respArr, c)
		}
	}()

	for _, region := range region.Regions {
		r := region
		g.Go(func() error {
			// 获取异常 Region
			var downRegionStoreArr []int

			for _, s := range r.DownPeers {
				for _, downStore := range downStores {
					if downStore == s.Peer.StoreID {
						downRegionStoreArr = append(downRegionStoreArr, s.Peer.StoreID)
					}
				}
			}

			// 异常副本数小于正常副本数
			if len(downRegionStoreArr) < (len(r.Peers) - len(downRegionStoreArr)) {
				var (
					tmpData []string
					resp    Resp
				)

				leaderByte, err := json.Marshal(r.Leader)
				if err != nil {
					return err
				}
				peerByte, err := json.Marshal(r.Peers)
				if err != nil {
					return err
				}
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(leaderByte), string(peerByte))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmpData
				// 修复建议
				for _, downRegionStore := range downRegionStoreArr {
					resp.Fixed = fmt.Sprintf(`operator add remove-peer %d %d`, r.ID, downRegionStore)
				}

				respChan <- resp
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	regionMap, err := engine.GetRegionStatus(regionType, regionIDS)
	if err != nil {
		return err
	}

	// region ID Map
	var (
		regionPanics []string
	)
	for _, resp := range respArr {
		if _, ok := regionMap[resp.RegionID]; ok {
			var (
				dbInfos []DBINFO
				tmpData []string
			)
			for _, m := range regionMap[resp.RegionID] {
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

			tmpData = append(resp.RegionInfo, string(pretty.Pretty(dbInfoByte)))
			data = append(data, tmpData)
		} else {
			regionPanics = append(regionPanics, resp.RegionID)
		}
	}

	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	// 修复建议
	for _, resp := range respArr {
		fmt.Printf("%s\n", resp.Fixed)
	}

	// 异常检测
	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
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

func getClusterDownRegion(pdAddr string) (DownRegion, error) {
	var region DownRegion

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
