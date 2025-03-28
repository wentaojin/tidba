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
package inspect

import (
	"gopkg.in/yaml.v3"
)

const (
	// size: seconds
	DefaultPdComponentRegionHeartbeatHandleLatencyUnderline = 0.030
	DefaultPdComponentHandleReuqestDurationUnderline        = 0.030
	DefaultPdComponentWalFsyncDurationUnderline             = 0.015

	// size: seconds
	DefaultTiDBComponentCommitTokenWaitDurationUnderline = 0.015

	// interger
	DefaultTiKVComponentSchedulerDiscardRatioUnderline = 0

	// size: seconds
	DefaultTiKVComponentWriteDiskLatencyUnderline = 0.015
	DefaultTiKVComponentReadDiskLatencyUnderline  = 0.015

	// size: seconds
	DefaultHostDiskWriteLatencyUnderline = 0.010
	DefaultHostDiskReadLatencyUnderline  = 0.010
)

type InspectConfig struct {
	WindowMinutes    int                    `yaml:"window_minutes" json:"window_minutes"`
	HostIPs          []string               `yaml:"host_ips" json:"host_ips"`
	VariablesParams  map[string]interface{} `yaml:"variables_params" json:"variables_params"`
	TiDBConfigParams map[string]interface{} `yaml:"tidb_config_params" json:"tidb_config_params"`
	PDConfigParams   map[string]interface{} `yaml:"pd_config_params" json:"pd_config_params"`
	TiKVConfigParams map[string]interface{} `yaml:"tikv_config_params" json:"tikv_config_params"`
	Modules          *Modules               `yaml:"modules" json:"modules"`
}

type Modules struct {
	CheckHardwareInfo          bool `yaml:"check_hardware_info" json:"check_hardware_info"`
	CheckSoftwareInfo          bool `yaml:"check_software_info" json:"check_software_info"`
	CheckTidbOverview          bool `yaml:"check_tidb_overview" json:"check_tidb_overview"`
	CheckDevBestPractices      bool `yaml:"check_dev_best_practices" json:"check_dev_best_practices"`
	CheckDbParams              bool `yaml:"check_db_params" json:"check_db_params"`
	CheckStatsBestPractices    bool `yaml:"check_stats_best_practices" json:"check_stats_best_practices"`
	CheckSysConfig             bool `yaml:"check_sys_config" json:"check_sys_config"`
	CheckCrontab               bool `yaml:"check_crontab" json:"check_crontab"`
	CheckDmesgLogs             bool `yaml:"check_dmesg_logs" json:"check_dmesg_logs"`
	CheckDbErrorLogs           bool `yaml:"check_db_error_logs" json:"check_db_error_logs"`
	CheckUserSpace             bool `yaml:"check_user_space" json:"check_user_space"`
	CheckPdPerformance         bool `yaml:"check_pd_performance" json:"check_pd_performance"`
	CheckTidbPerformance       bool `yaml:"check_tidb_performance" json:"check_tidb_performance"`
	CheckTikvPerformance       bool `yaml:"check_tikv_performance" json:"check_tikv_performance"`
	CheckSQLOrderByElapsedTime bool `yaml:"check_sql_order_by_elapsed_time" json:"check_sql_order_by_elapsed_time"`
	CheckSQLOrderByTidbCPUTime bool `yaml:"check_sql_order_by_tidb_cpu_time" json:"check_sql_order_by_tidb_cpu_time"`
	CheckSQLOrderByTikvCPUTime bool `yaml:"check_sql_order_by_tikv_cpu_time" json:"check_sql_order_by_tikv_cpu_time"`
	CheckSQLOrderByExecutions  bool `yaml:"check_sql_order_by_executions" json:"check_sql_order_by_executions"`
	CheckSQLOrderByPlans       bool `yaml:"check_sql_order_by_plans" json:"check_sql_order_by_plans"`
}

func DefaultInspectConfigTemplate() *InspectConfig {
	return &InspectConfig{
		WindowMinutes: 2880,
		HostIPs:       []string{},
		VariablesParams: map[string]interface{}{
			"tidb_enable_rate_limit_action":               "off",
			"tidb_distsql_scan_concurrency":               20,
			"tidb_mem_quota_query":                        104857600,
			"sql_require_primary_key":                     "on",
			"max_execution_time":                          60000,
			"tidb_scatter_region":                         "on",
			"tidb_prepared_plan_cache_size":               100,
			"tidb_prepared_plan_cache_memory_guard_ratio": 0.1,
			"tidb_tmp_table_max_size":                     134217728,
			"tidb_gc_life_time":                           "2h",
			"tidb_ddl_enable_fast_reorg":                  "OFF",
			"tidb_opt_fix_control":                        "44262:ON",
		},
		TiDBConfigParams: map[string]interface{}{
			"log.level":                                 "info",
			"log.slow-threshold":                        2000,
			"log.file.max-days":                         30,
			"log.file.max-size":                         300,
			"log.file.max-backups":                      150,
			"new_collations_enabled_on_first_bootstrap": false,
			"table-column-count-limit":                  4096,
			"max-index-length":                          12000,
			"max_connections":                           2000,
			"tmp-storage-quota":                         53687091200,
			"performance.max-procs":                     20,
			"performance.max-txn-ttl":                   3600000,
			"instance.tidb_slow_log_threshold":          2000,
		},
		PDConfigParams: map[string]interface{}{
			"log.file.max-size":           300,
			"log.file.max-days":           30,
			"log.file.max-backups":        150,
			"replication.location-labels": "zone,host",
		},
		TiKVConfigParams: map[string]interface{}{
			"log.level":                             "info",
			"log.format":                            "text",
			"log.enable-timestamp":                  true,
			"log.file.max-size":                     300,
			"log.file.max-days":                     30,
			"log.file.max-backups":                  150,
			"pessimistic-txn.pipelined":             false,
			"pessimistic-txn.in-memory":             false,
			"raftstore.apply-max-batch-size":        1024,
			"raftstore.apply-pool-size":             4,
			"raftstore.store-pool-size":             4,
			"readpool.coprocessor.use-unified-pool": true,
			"readpool.storage.high-concurrency":     12,
			"readpool.storage.low-concurrency":      12,
			"readpool.storage.normal-concurrency":   12,
			"readpool.storage.use-unified-pool":     false,
			"storage.block-cache.capacity":          "96GiB",
		},
		Modules: &Modules{
			CheckHardwareInfo:          true,
			CheckSoftwareInfo:          true,
			CheckTidbOverview:          true,
			CheckDevBestPractices:      true,
			CheckDbParams:              true,
			CheckStatsBestPractices:    true,
			CheckSysConfig:             true,
			CheckCrontab:               true,
			CheckDmesgLogs:             true,
			CheckDbErrorLogs:           true,
			CheckUserSpace:             true,
			CheckPdPerformance:         true,
			CheckTidbPerformance:       true,
			CheckTikvPerformance:       true,
			CheckSQLOrderByElapsedTime: true,
			CheckSQLOrderByTidbCPUTime: true,
			CheckSQLOrderByTikvCPUTime: true,
			CheckSQLOrderByExecutions:  true,
			CheckSQLOrderByPlans:       true,
		},
	}
}

func (i *InspectConfig) String() string {
	conf, err := yaml.Marshal(i)
	if err != nil {
		panic(err)
	}
	return string(conf)
}
