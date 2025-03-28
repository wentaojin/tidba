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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Region struct {
	Count   int             `json:"count"`
	Regions []*SingleRegion `json:"regions"`
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
	WrittenBytes    int       `json:"written_bytes"`
	ReadBytes       int       `json:"read_bytes"`
	WrittenKeys     int       `json:"written_keys"`
	ReadKeys        int       `json:"read_keys"`
	ApproximateSize int       `json:"approximate_size"`
	ApproximateKeys int       `json:"approximate_keys"`
	DbINFO          []*DbINFO `json:"db_info"`
}

type DbINFO struct {
	RegionID  string `json:"region_id"`
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
	StartKey  string `json:"start_key"`
	EndKey    string `json:"end_key"`
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
			LeaderScore     float64   `json:"leader_score"`
			LeaderSize      float64   `json:"leader_size"`
			RegionCount     int       `json:"region_count"`
			RegionWeight    int       `json:"region_weight"`
			RegionScore     float64   `json:"region_score"`
			RegionSize      float64   `json:"region_size"`
			SlowScore       int       `json:"slow_score"`
			StartTs         time.Time `json:"start_ts"`
			LastHeartbeatTs time.Time `json:"last_heartbeat_ts"`
			Uptime          string    `json:"uptime"`
		} `json:"status"`
	} `json:"stores"`
}

type Response struct {
	RegionID   string
	RegionInfo []string
}

func getClusterRegions(pdAddr string) (*Region, error) {
	var region *Region

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

func getClusterConfigReplica(pdAddr string) (*ConfigReplica, error) {
	var cfgReplica *ConfigReplica
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

func getClusterSingleRegion(pdAddr string, regionID string) (*SingleRegion, error) {
	var region *SingleRegion

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

func getClusterStores(pdAddr string) (*Store, error) {
	var store *Store

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
