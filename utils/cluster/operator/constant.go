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
package operator

const (
	ComponentNameUbiSQL       = "ubisql"
	ComponentNameTiDB         = "tidb"
	ComponentNameTiKV         = "tikv"
	ComponentNamePD           = "pd"
	ComponentNameTiCDC        = "cdc"
	ComponentNameTiSpark      = "tispark"
	ComponentNameTiFlash      = "tiflash"
	ComponentNameAlertmanager = "alertmanager"
	ComponentNameGrafana      = "grafana"
	ComponentNamePrometheus   = "prometheus"
)

const (
	UbiSQLClusterVersionV101 = "v1.0.1"
	UbiSQLClusterVersionV102 = "v1.0.2"
	UbiSQLClusterVersionV110 = "v1.1.0"
	UbiSQLClusterVersionV200 = "v2.0.0"
	UbiSQLClusterVersionV201 = "v2.0.1"
	UbiSQLClusterVersionV300 = "v3.0.0"
	UbiSQLClusterVersionV310 = "v3.1.0"
	UbiSQLClusterVersionV311 = "v3.1.1"
	UbiSQLClusterVersionV400 = "v4.0.0"
	UbiSQLClusterVersionV500 = "v5.0.0"
)

// UbiSQLComponentMappingTiDBComponentVersion 映射 UbiSQL 组件到 TiDB 组件版本
var UbiSQLComponentMappingTiDBComponentVersion = map[string]string{
	UbiSQLClusterVersionV101: "v4.0.10",
	UbiSQLClusterVersionV102: "v4.0.15",
	UbiSQLClusterVersionV110: "v4.0.15",
	UbiSQLClusterVersionV200: "v5.4.0",
	UbiSQLClusterVersionV201: "v5.4.1",
	UbiSQLClusterVersionV300: "v6.5.1",
	UbiSQLClusterVersionV310: "v6.5.3",
	UbiSQLClusterVersionV311: "v6.5.7",
	UbiSQLClusterVersionV400: "v7.5.0",
	UbiSQLClusterVersionV500: "v8.5.1",
}
