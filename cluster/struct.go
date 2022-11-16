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

import "time"

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

type DownRegion struct {
	Count   int `json:"count"`
	Regions []struct {
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
		DownPeers []struct {
			DownSeconds int `json:"down_seconds"`
			Peer        struct {
				ID       int    `json:"id"`
				StoreID  int    `json:"store_id"`
				RoleName string `json:"role_name"`
			} `json:"peer"`
		} `json:"down_peers"`
		WrittenBytes    int `json:"written_bytes"`
		ReadBytes       int `json:"read_bytes"`
		WrittenKeys     int `json:"written_keys"`
		ReadKeys        int `json:"read_keys"`
		ApproximateSize int `json:"approximate_size"`
		ApproximateKeys int `json:"approximate_keys"`
	} `json:"regions"`
}

type Resp struct {
	RegionID   string
	RegionInfo []string
	Fixed      []string
}
