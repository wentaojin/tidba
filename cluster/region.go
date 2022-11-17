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
	"sync"
	"time"
)

func GetCurrentRegionPeer(peers string, regionType, pdAddr string, concurrency int, engine *db.Engine) error {
	startTime := time.Now()
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[1/5][region] get down region peer info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[2/5][region] get cluster config replica info... cost:[%v]\n", time.Now().Sub(startTime))

	var (
		regionID []string
	)
	regionMaps := &sync.Map{}

	g := &errgroup.Group{}
	g.SetLimit(concurrency)
	tmpRegionMapChan := make(chan Resp)

	go func() {
		for c := range tmpRegionMapChan {
			regionID = append(regionID, c.RegionID)
			regionMaps.Store(c.RegionID, c.RegionInfo)
		}
	}()
	for _, res := range region.Regions {
		r := res
		g.Go(func() error {
			// 获取指定副本数 Region
			peerInt, err := strconv.Atoi(peers)
			if err != nil {
				return err
			}
			if len(r.Peers) == peerInt {
				var tmpData []string
				leaderByte, err := json.Marshal(r.Leader)
				if err != nil {
					return err
				}
				peerByte, err := json.Marshal(r.Peers)
				if err != nil {
					return err
				}
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(leaderByte), string(peerByte))
				tmpRegionMapChan <- Resp{
					RegionID:   strconv.Itoa(r.ID),
					RegionInfo: tmpData,
				}
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	fmt.Printf("[3/5][region] get major down peer info... cost:[%v]\n", time.Now().Sub(startTime))

	// 获取 region 库表/索引信息
	if len(regionID) == 0 {
		fmt.Printf(">--------+  Replica Peer [%s] Isn't Exist +--------<\n", peers)
		return nil
	}

	startTime = time.Now()
	regionMap, err := engine.GetRegionStatus(regionType, regionID, concurrency)
	if err != nil {
		return err
	}

	fmt.Printf("[4/5][region] get major down peer status... cost:[%v]\n", time.Now().Sub(startTime))

	// region id map
	var (
		data         [][]string
		regionPanics []string
	)

	startTime = time.Now()

	g2 := errgroup.Group{}
	g2.SetLimit(concurrency)

	dataChan := make(chan []string, concurrency)
	regionPanicChan := make(chan string, concurrency)

	go func() {
		for c := range dataChan {
			data = append(data, c)
		}
	}()

	go func() {
		for c := range regionPanicChan {
			regionPanics = append(regionPanics, c)
		}
	}()

	regionMap.Range(func(key, value any) bool {
		k := key
		v := value
		g2.Go(func() error {
			// region ID Map
			if vs, ok := regionMaps.Load(k); ok {
				var (
					dbInfos []DBINFO
					tmpData []string
				)
				for _, m := range v.([]string) {
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
				tmpData = append(tmpData, vs.(string), string(pretty.Pretty(dbInfoByte)))
				dataChan <- tmpData
			} else {
				regionPanicChan <- k.(string)
			}
			return nil
		})
		return true
	})

	if err = g2.Wait(); err != nil {
		return err
	}

	fmt.Printf("[5/5][region] gen major down peer table... cost:[%v]\n", time.Now().Sub(startTime))

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)
	columns := []string{"RegionID", "LeaderID", "Peers", "Category"}
	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}
	return nil
}

func GetMajorDownRegionPeer(regionType, pdAddr string, concurrency int, engine *db.Engine) error {
	startTime := time.Now()
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[1/6][region] get down region peer info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[2/6][region] get cluster config replica info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	allStores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}
	downStores := make(map[int]struct{})
	for _, s := range allStores.Stores {
		if strings.EqualFold(s.Store.StateName, "DOWN") {
			downStores[s.Store.ID] = struct{}{}
		}
	}

	fmt.Printf("[3/6][region] get cluster down store info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		regionIDS []string
	)
	regionMaps := &sync.Map{}

	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			regionMaps.Store(c.RegionID, c.RegionInfo)
		}
	}()

	for _, res := range region.Regions {
		r := res
		g.Go(func() error {
			// 获取异常 Region
			var downRegionArr []int

			for _, ds := range r.Peers {
				if _, ok := downStores[ds.StoreID]; ok {
					downRegionArr = append(downRegionArr, r.ID)
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
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(pretty.Pretty(leaderByte)), string(pretty.Pretty(peerByte)))

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

	fmt.Printf("[4/6][region] get major down peer info... cost:[%v]\n", time.Now().Sub(startTime))

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	startTime = time.Now()
	regionMap, err := engine.GetRegionStatus(regionType, regionIDS, concurrency)
	if err != nil {
		return err
	}
	fmt.Printf("[5/6][region] get major down peer status... cost:[%v]\n", time.Now().Sub(startTime))

	// region ID Map
	// 忽略已被删除但未 GC 的 Region 信息 （tidb 接口内查询结果为空的 region）

	var (
		data         [][]string
		regionPanics []string
	)

	startTime = time.Now()

	g2 := errgroup.Group{}
	g2.SetLimit(concurrency)

	dataChan := make(chan []string, concurrency)
	regionPanicChan := make(chan string, concurrency)

	go func() {
		for c := range dataChan {
			data = append(data, c)
		}
	}()

	go func() {
		for c := range regionPanicChan {
			regionPanics = append(regionPanics, c)
		}
	}()

	regionMap.Range(func(key, value any) bool {
		kRegion := key
		vRegion := value
		g2.Go(func() error {
			if vs, ok := regionMaps.Load(kRegion); ok {
				var (
					dbInfos []DBINFO
					tmpData []string
				)
				for _, m := range vRegion.([]string) {
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

				tmpData = append(tmpData, vs.([]string)...)
				tmpData = append(tmpData, string(pretty.Pretty(dbInfoByte)))
				dataChan <- tmpData
			} else {
				regionPanicChan <- kRegion.(string)
			}
			return nil
		})
		return true
	})

	if err := g2.Wait(); err != nil {
		return err
	}

	fmt.Printf("[6/6][region] gen major down peer table... cost:[%v]\n", time.Now().Sub(startTime))

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)

	columns := []string{"RegionID", "Leader", "Peers", "Category"}
	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}

	fmt.Printf(">--------+  Program Region Result Count: %v  +--------<\n", len(data))

	return nil
}

func GetMajorDownRegionPeerOverview(regionType, pdAddr string, concurrency int, engine *db.Engine) error {
	startTime := time.Now()
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[1/6][region] get down region peer info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[2/6][region] get cluster config replica info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	allStores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}
	downStores := make(map[int]struct{})
	for _, s := range allStores.Stores {
		if strings.EqualFold(s.Store.StateName, "DOWN") {
			downStores[s.Store.ID] = struct{}{}
		}
	}

	fmt.Printf("[3/6][region] get cluster down store info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		regionIDS []string
	)
	regionMaps := &sync.Map{}

	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			regionMaps.Store(c.RegionID, c.RegionInfo)
		}
	}()

	for _, res := range region.Regions {
		r := res
		g.Go(func() error {
			// 获取异常 Region
			var downRegionArr []int

			for _, ds := range r.Peers {
				if _, ok := downStores[ds.StoreID]; ok {
					downRegionArr = append(downRegionArr, r.ID)
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
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(pretty.Pretty(leaderByte)), string(pretty.Pretty(peerByte)))

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

	fmt.Printf("[4/6][region] get major down peer info... cost:[%v]\n", time.Now().Sub(startTime))

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	startTime = time.Now()

	// 忽略已被删除但未 GC 的 Region 信息 （tidb 接口内查询结果为空的 region）
	regionData, err := engine.GetRegionStatusGroupByTable(regionType, regionIDS, concurrency)
	if err != nil {
		return err
	}
	fmt.Printf("[5/6][region] get major down peer status... cost:[%v]\n", time.Now().Sub(startTime))

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)

	columns := []string{"DB_NAME", "TABLE_NAME", "INDEX_NAME", "DOWN_REGIONS"}
	util.QueryResultFormatTableWithRowLineStyle(columns, regionData)

	fmt.Printf(">--------+  Program Table Result Count: %v  +--------<\n", len(regionData))
	return nil
}

func GetMajorDownRegionPeerByDownTiKVS(regionType, pdAddr string, concurrency int, downTiKVS []string, engine *db.Engine) error {
	downStores := make(map[int]struct{})

	startTime := time.Now()
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[1/6][region] get down region peer info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[2/6][region] get cluster config replica info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	stores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}

	for _, t := range downTiKVS {
		for _, s := range stores.Stores {
			if strings.EqualFold(t, s.Store.Address) && strings.EqualFold(s.Store.StateName, "DOWN") {
				downStores[s.Store.ID] = struct{}{}
			}
		}
	}
	fmt.Printf("[3/6][region] get cluster stores info... cost:[%v]\n", time.Now().Sub(startTime))

	if len(downStores) != len(downTiKVS) {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check down store [%v]", downTiKVS, downStores)
	}

	startTime = time.Now()
	g1 := errgroup.Group{}
	g1.SetLimit(concurrency)

	var (
		regionIDS []string
	)

	regionMaps := &sync.Map{}

	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			regionMaps.Store(c.RegionID, c.RegionInfo)
		}
	}()

	for _, res := range region.Regions {
		r := res
		g1.Go(func() error {
			// 获取异常 Region
			var downRegionArr []int

			for _, ds := range r.Peers {
				if _, ok := downStores[ds.StoreID]; ok {
					downRegionArr = append(downRegionArr, r.ID)
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
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(pretty.Pretty(leaderByte)), string(pretty.Pretty(peerByte)))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmpData

				respChan <- resp
			}
			return nil
		})
	}

	if err = g1.Wait(); err != nil {
		return err
	}

	fmt.Printf("[4/6][region] get major down peer info... cost:[%v]\n", time.Now().Sub(startTime))

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}
	startTime = time.Now()
	regionMap, err := engine.GetRegionStatus(regionType, regionIDS, concurrency)
	if err != nil {
		return err
	}
	fmt.Printf("[5/6][region] get major down peer status... cost:[%v]\n", time.Now().Sub(startTime))

	// region ID Map
	// 忽略已被删除但未 GC 的 Region 信息 （tidb 接口内查询结果为空的 region）
	var (
		data         [][]string
		regionPanics []string
	)

	startTime = time.Now()

	g2 := errgroup.Group{}
	g2.SetLimit(concurrency)

	dataChan := make(chan []string, concurrency)
	regionPanicChan := make(chan string, concurrency)

	go func() {
		for c := range dataChan {
			data = append(data, c)
		}
	}()

	go func() {
		for c := range regionPanicChan {
			regionPanics = append(regionPanics, c)
		}
	}()

	regionMap.Range(func(key, value any) bool {
		kRegion := key
		vRegion := value
		g2.Go(func() error {
			if vs, ok := regionMaps.Load(kRegion); ok {
				var (
					dbInfos []DBINFO
					tmpData []string
				)
				for _, m := range vRegion.([]string) {
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

				tmpData = append(tmpData, vs.([]string)...)
				tmpData = append(tmpData, string(pretty.Pretty(dbInfoByte)))
				dataChan <- tmpData
			} else {
				regionPanicChan <- kRegion.(string)
			}
			return nil

		})
		return true
	})

	if err := g2.Wait(); err != nil {
		return err
	}

	fmt.Printf("[6/6][region] gen major down peer table... cost:[%v]\n", time.Now().Sub(startTime))

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)

	columns := []string{"RegionID", "Leader", "Peers", "Category"}
	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}
	fmt.Printf(">--------+  Program Region Result Count: %v  +--------<\n", len(data))
	return nil
}

func GetLessDownRegionPeerByDownTiKVS(regionType, pdAddr string, concurrency int, downTiKVS []string, engine *db.Engine) error {

	downStores := make(map[int]struct{})

	startTime := time.Now()
	region, err := getClusterAllRegion(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[1/6][region] get down region peer info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	cfgReplica, err := getClusterConfigReplica(pdAddr)
	if err != nil {
		return err
	}
	fmt.Printf("[2/6][region] get cluster config replica info... cost:[%v]\n", time.Now().Sub(startTime))

	startTime = time.Now()
	stores, err := getClusterStores(pdAddr)
	if err != nil {
		return err
	}

	for _, t := range downTiKVS {
		for _, s := range stores.Stores {
			if strings.EqualFold(t, s.Store.Address) && strings.EqualFold(s.Store.StateName, "DOWN") {
				downStores[s.Store.ID] = struct{}{}
			}
		}
	}
	fmt.Printf("[3/6][region] get cluster stores info... cost:[%v]\n", time.Now().Sub(startTime))

	if len(downStores) != len(downTiKVS) {
		return fmt.Errorf("down tikv [%v] isn't exist, please again check down store [%v]", downTiKVS, downStores)
	}

	startTime = time.Now()
	g := errgroup.Group{}
	g.SetLimit(concurrency)

	var (
		regionIDS  []string
		regionFixs []string
	)
	regionMaps := &sync.Map{}
	respChan := make(chan Resp, concurrency)

	go func() {
		for c := range respChan {
			regionIDS = append(regionIDS, c.RegionID)
			regionMaps.Store(c.RegionID, c.RegionInfo)
			regionFixs = append(regionFixs, c.Fixed...)
		}
	}()

	for _, res := range region.Regions {
		r := res
		g.Go(func() error {
			// 获取异常 Region
			var downRegionStoreArr []int

			for _, s := range r.Peers {
				if _, ok := downStores[s.StoreID]; ok {
					downRegionStoreArr = append(downRegionStoreArr, s.StoreID)
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
				tmpData = append(tmpData, strconv.Itoa(r.ID), string(pretty.Pretty(leaderByte)), string(pretty.Pretty(peerByte)))

				resp.RegionID = strconv.Itoa(r.ID)
				resp.RegionInfo = tmpData
				// 修复建议
				for _, downRegionStore := range downRegionStoreArr {
					resp.Fixed = append(resp.Fixed, fmt.Sprintf(`operator add remove-peer %d %d`, r.ID, downRegionStore))
				}

				respChan <- resp
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	fmt.Printf("[4/6][region] get major down peer info... cost:[%v]\n", time.Now().Sub(startTime))

	// 获取 region 库表/索引信息
	if len(regionIDS) == 0 {
		fmt.Println(">--------+  Replica Peer Is Normally +--------<")
		return nil
	}

	startTime = time.Now()
	regionMap, err := engine.GetRegionStatus(regionType, regionIDS, concurrency)
	if err != nil {
		return err
	}
	fmt.Printf("[5/6][region] get major down peer status... cost:[%v]\n", time.Now().Sub(startTime))

	// region ID Map
	// 忽略已被删除但未 GC 的 Region 信息 （tidb 接口内查询结果为空的 region）

	var (
		data         [][]string
		regionPanics []string
	)

	startTime = time.Now()

	g2 := errgroup.Group{}
	g2.SetLimit(concurrency)

	dataChan := make(chan []string, concurrency)
	regionPanicChan := make(chan string, concurrency)

	go func() {
		for c := range dataChan {
			data = append(data, c)
		}
	}()

	go func() {
		for c := range regionPanicChan {
			regionPanics = append(regionPanics, c)
		}
	}()

	regionMap.Range(func(key, value any) bool {
		kRegion := key
		vRegion := value
		g2.Go(func() error {
			if vs, ok := regionMaps.Load(kRegion); ok {
				var (
					dbInfos []DBINFO
					tmpData []string
				)
				for _, m := range vRegion.([]string) {
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

				tmpData = append(tmpData, vs.([]string)...)
				tmpData = append(tmpData, string(pretty.Pretty(dbInfoByte)))
				dataChan <- tmpData
			} else {
				regionPanicChan <- kRegion.(string)
			}
			return nil

		})
		return true
	})

	if err := g2.Wait(); err != nil {
		return err
	}
	fmt.Printf("[6/6][region] gen major down peer table... cost:[%v]\n", time.Now().Sub(startTime))

	fmt.Printf(">--------+  Cluster Replica Count: %v  +--------<\n", cfgReplica.MaxReplicas)
	fmt.Printf(">--------+  Down Region Count: %v  +--------<\n", region.Count)

	columns := []string{"RegionID", "Leader", "Peers", "Category"}
	util.QueryResultFormatTableWithRowLineStyle(columns, data)

	// 修复建议
	for _, fixed := range regionFixs {
		fmt.Printf("%s\n", fixed)
	}

	// 异常检测
	if len(regionPanics) > 0 {
		fmt.Printf("region id [%s] query sql result isn't exist, please again check", regionPanics)
	}

	fmt.Printf(">--------+  Program Region Result Count: %v  +--------<\n", len(data))

	return nil
}

func GetRegionIDINFO(regionID []string, regionType, pdAddr string, concurrency int, engine *db.Engine) error {
	// 获取 region 库表/索引信息
	regionMap, err := engine.GetRegionStatus(regionType, regionID, concurrency)
	if err != nil {
		return err
	}

	for _, r := range regionID {
		var singleRegion SingleRegion
		singleRegion, err = getClusterSingleRegion(pdAddr, r)
		if err != nil {
			return err
		}
		if _, ok := regionMap.Load(r); ok {
			fmt.Printf(">--------+  Region ID: %v  +--------<\n", r)

			var dbInfos []DBINFO
			regionMap.Range(func(key, value any) bool {
				var dbInfo DBINFO
				if err = json.Unmarshal([]byte(strings.Join(value.([]string), ",")), &dbInfo); err != nil {
					return false
				}
				dbInfos = append(dbInfos, dbInfo)
				return true
			})
			if err != nil {
				return err
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
