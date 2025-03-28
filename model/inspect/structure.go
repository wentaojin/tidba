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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
)

type Report struct {
	*ReportBody
	*ReportSummary
	*ReportDetail
	*ReportAbnormal
}

type ReportBody struct {
	ClusterName    string
	ClusterVersion string
	InspectionTime string
}

func (rs *ReportBody) String() string {
	jsStr, _ := json.Marshal(rs)
	return string(jsStr)
}

type ReportSummary struct {
	InspectSummary []*InspectSummary
}

func (rs *ReportSummary) String() string {
	jsStr, _ := json.Marshal(rs)
	return string(jsStr)
}

type ReportDetail struct {
	InspectionWindowHour         float64
	BasicHardwares               []*BasicHardware
	BasicSoftwares               []*BasicSoftware
	ClusterTopologys             []*ClusterTopology
	ClusterSummarys              []*ClusterSummary
	DevBestPractices             []*DevBestPractice
	DatabaseVaribales            []*DatabaseVaribale
	DatabaseConfigs              []*DatabaseConfig
	DatabaseStatistics           []*DatabaseStatistics
	SystemConfigs                []*SystemConfig
	SystemConfigOutputs          []*SystemConfigOutput
	SystemCrontabs               []*SystemCrontab
	SystemDmesgs                 []*SystemDmesg
	DatabaseErrorCounts          []*DatabaseErrorCount
	DatabaseSchemaSpaces         []*DatabaseSchemaSpace
	DatabaseTableSpaceTops       []*DatabaseTableSpaceTop
	PerformanceStatisticsByPds   []*PerformanceStatisticsByPD
	PerformanceStatisticsByTidbs []*PerformanceStatisticsByTiDB
	PerformanceStatisticsByTikvs []*PerformanceStatisticsByTiKV
	SqlOrderedByElapsedTimes     []*SqlOrderedByElapsedTime
	SqlOrderedByTiDBCpuTimes     []*SqlOrderedByTiDBCpuTime
	SqlOrderedByTiKVCpuTimes     []*SqlOrderedByTiKVCpuTime
	SqlOrderedByExecutions       []*SqlOrderedByExecution
	SqlOrderedByPlans            []*SqlOrderedByPlan
}

func (rs *ReportDetail) String() string {
	jsStr, _ := json.Marshal(rs)
	return string(jsStr)
}

type ReportAbnormal struct {
	DevAbnormals   []*InspDevBestPracticesAbnormalOutput
	StatsAbnormals []*InspDatabaseStatisticsAbnormalOutput
}

type InspectSummary struct {
	SummaryName   string
	IsPanic       bool
	SummaryResult string
}

func GenReportSummary(r *ReportDetail) *ReportSummary {
	clusterSummaryPanic := 0
	devBestSummaryPanic := 0
	dbParamsSummaryPanic := 0
	dbStatisSummaryPanic := 0
	sysConfigSummaryPanic := 0
	sysDmesgSummaryPanic := 0
	dbErrSummaryPanic := 0
	for _, t := range r.ClusterSummarys {
		if t.CheckResult == "正常" {
			continue
		}
		clusterSummaryPanic++
	}
	for _, t := range r.DevBestPractices {
		if t.CheckResult == "正常" {
			continue
		}
		devBestSummaryPanic++
	}
	for _, t := range r.DatabaseVaribales {
		if t.IsStandard == "否" {
			dbParamsSummaryPanic++
		}
	}
	for _, t := range r.DatabaseConfigs {
		if t.IsStandard == "否" {
			dbParamsSummaryPanic++
		}
	}
	for _, t := range r.DatabaseStatistics {
		if t.CheckResult == "正常" {
			continue
		}
		dbStatisSummaryPanic++
	}
	sysConfigSummaryPanic = len(r.SystemConfigOutputs)

	for _, t := range r.SystemDmesgs {
		if t.AbnormalStatus == "正常" {
			continue
		}
		sysDmesgSummaryPanic++
	}
	for _, t := range r.DatabaseErrorCounts {
		if t.ErrorCount != "" {
			dbErrSummaryPanic++
		}
	}

	var summaries []*InspectSummary
	for _, s := range DefaultReportSummaryContent() {
		var sm = &InspectSummary{}
		sm.SummaryName = s.SummaryName
		sm.IsPanic = false
		sm.SummaryResult = s.SummaryResult

		if s.SummaryName == "3.3 TiDB 集群总览" && clusterSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", clusterSummaryPanic)
		}
		if s.SummaryName == "3.4 开发规范最佳实践" && devBestSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", devBestSummaryPanic)
		}
		if s.SummaryName == "3.5 数据库参数最佳实践" && dbParamsSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", dbParamsSummaryPanic)
		}
		if s.SummaryName == "3.6 统计信息最佳实践" && dbStatisSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", dbStatisSummaryPanic)
		}
		if s.SummaryName == "3.7 系统配置最佳实践" && sysConfigSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", sysConfigSummaryPanic)
		}
		if s.SummaryName == "3.9 dmesg 情况" && sysDmesgSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", sysDmesgSummaryPanic)
		}
		if s.SummaryName == "3.10 数据库的错误日志统计" && dbErrSummaryPanic > 0 {
			sm.IsPanic = true
			sm.SummaryResult = fmt.Sprintf("告警（with %d error）", dbErrSummaryPanic)
		}
		summaries = append(summaries, sm)
	}

	return &ReportSummary{
		InspectSummary: summaries,
	}
}

func (r *Report) String() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
}

type BasicHardware struct {
	IpAddress string
	CpuArch   string
	CpuVcore  string
	Numa      string
	Memory    string
	OsVersion string
}

type BasicSoftware struct {
	Category string
	Value    string
}

type ClusterTopology struct {
	IpAddress  string
	Components string
}

type ClusterSummary struct {
	CheckItem     string
	CheckBaseline string
	CheckResult   string
	ResultDesc    string
}

type DevBestPractice struct {
	CheckItem         string
	CheckCategory     string
	CorrectionSuggest string
	BestPracticeDesc  string
	CheckResult       string
	AbnormalDetail    string
}

type DatabaseVaribale struct {
	Component     string
	ParamName     string
	DefaultValue  string
	CurrentValue  string
	StandardValue string
	IsStandard    string
}

type DatabaseConfig struct {
	Component     string
	Instance      string
	ParamName     string
	CurrentValue  string
	StandardValue string
	IsStandard    string
}

type DatabaseStatistics struct {
	CheckItem      string
	CheckStandard  string
	CheckResult    string
	AbnormalDetail string
}

type SystemConfig struct {
	CheckItem     string
	CheckStandard string
}

type SystemConfigOutput struct {
	IpAddress      string
	AbnormalDetail string
}

type SystemCrontab struct {
	IpAddress      string
	CrontabUser    string
	CrontabContent string
}

type SystemDmesg struct {
	IpAddress      string
	AbnormalStatus string
	AbnormalDetail string
}

type DatabaseErrorCount struct {
	InstAddress string
	Component   string
	ErrorCount  string
}

type DatabaseSchemaSpace struct {
	SchemaName   string
	IndexSpaceGB string
	DataSpaceGB  string
	TotalSpaceGB string
}

type DatabaseTableSpaceTop struct {
	SchemaName   string
	TableName    string
	RowCounts    string
	ColumnCounts string
	TotalSpaceGB string
}

type PerformanceStatisticsByPD struct {
	PDInstance      string
	MonitoringItems string
	AvgMetrics      string
	MaxMetrics      string
	ParamValue      string
	SuggestValue    string
	Comment         string
}

type PerformanceStatisticsByTiDB struct {
	TiDBInstance    string
	MonitoringItems string
	AvgMetrics      string
	MaxMetrics      string
	ParamValue      string
	SuggestValue    string
	Comment         string
}

type PerformanceStatisticsByTiKV struct {
	TiKVInstance    string
	MonitoringItems string
	AvgMetrics      string
	MaxMetrics      string
	ParamValue      string
	SuggestValue    string
	Comment         string
}

type SqlOrderedByElapsedTime struct {
	ElapsedTime       string
	Executions        string
	ElapPerExec       string
	MinQueryTime      string
	MaxQueryTime      string
	AvgTotalKeys      string
	AvgProcessedKeys  string
	SqlTimePercentage string
	SqlDigest         string
	SqlText           string
}

type SqlOrderedByTiDBCpuTime struct {
	CpuTimeSec        string
	ExecCountsPerSec  string
	LatencyPerExec    string
	ScanRecordPerSec  uint64
	ScanIndexesPerSec uint64
	PlanCounts        int
	SqlDigest         string
	SqlText           string
}

type SqlOrderedByTiKVCpuTime struct {
	CpuTimeSec        string
	ExecCountsPerSec  string
	LatencyPerExec    string
	ScanRecordPerSec  uint64
	ScanIndexesPerSec uint64
	PlanCounts        int
	SqlDigest         string
	SqlText           string
}

type SqlOrderedByExecution struct {
	Executions        string
	ElapPerExec       string
	ParsePerExec      string
	CompilePerExec    string
	MinQueryTime      string
	MaxQueryTime      string
	AvgTotalKeys      string
	AvgProcessedKeys  string
	SqlTimePercentage string
	SqlDigest         string
	SqlText           string
}

type SqlOrderedByPlan struct {
	SqlPlans          string
	ElapsedTime       string
	Executions        string
	MinSqlPlan        string
	MaxSqlPlan        string
	AvgTotalKeys      string
	AvgProcessedKeys  string
	SqlTimePercentage string
	SqlDigest         string
	SqlText           string
}

// topsql
type TopSql struct {
	mu   sync.Mutex
	Data []*Sql `json:"data"`
}

func NewTopSqls() *TopSql {
	return &TopSql{
		mu:   sync.Mutex{},
		Data: make([]*Sql, 0),
	}
}

func (tsql *TopSql) Append(data ...*Sql) {
	tsql.mu.Lock()
	defer tsql.mu.Unlock()
	tsql.Data = append(tsql.Data, data...)
}

type Sql struct {
	SqlDigest         string  `json:"sql_digest"`
	SqlText           string  `json:"sql_text"`
	IsOther           bool    `json:"is_other"`
	CpuTimeMs         float64 `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	DurationPerExecMs float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec uint64  `json:"scan_records_per_sec"`
	ScanIndexesPerSec uint64  `json:"scan_indexes_per_sec"`
	Plans             []*Plan `json:"plans"`
}

type Plan struct {
	PlanDigest        string   `json:"plan_digest"`
	PlanText          string   `json:"plan_text"`
	TimestampSec      []uint64 `json:"timestamp_sec"`
	CpuTimeMs         []uint64 `json:"cpu_time_ms"`
	ExecCountPerSec   float64  `json:"exec_count_per_sec"`
	DurationPerExecMs float64  `json:"duration_per_exec_ms"`
	ScanRecordsPerSec uint64   `json:"scan_records_per_sec"`
	ScanIndexesPerSec uint64   `json:"scan_indexes_per_sec"`
}

type TopComponentSql struct {
	CpuTimeSec        float64 `json:"cpu_time_sec"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	LatencyPerExecSec float64 `json:"latency_per_exec_sec"`
	ScanRecordsPerSec uint64  `json:"scan_records_per_sec"`
	ScanIndexesPerSec uint64  `json:"scan_indexes_per_sec"`
	PlanCounts        int     `json:"plan_counts"`
	SqlDigest         string  `json:"sql_digest"`
	SqlText           string  `json:"sql_text"`
}

func (t *TopSql) ComputeComponentCpuTime() []*TopComponentSql {
	var tdSqls []*TopComponentSql

	groupBySqlDigest := make(map[string][]*Sql)
	for _, t := range t.Data {
		// exclude after sql top digest
		if t.SqlDigest != "" && !t.IsOther {
			groupBySqlDigest[t.SqlDigest] = append(groupBySqlDigest[t.SqlDigest], t)
		}
	}

	for _, sqls := range groupBySqlDigest {
		var (
			cpuTimeMs         float64
			execCountPerSec   float64
			durationPerExecMs float64
			scanRecordsPerSec uint64
			scanIndexesPerSec uint64
		)
		tsql := &TopComponentSql{}

		groupByPlanDigest := make(map[string][]*Plan)

		for _, s := range sqls {
			cpuTimeMs += s.CpuTimeMs
			execCountPerSec += s.ExecCountPerSec
			durationPerExecMs += s.DurationPerExecMs
			scanRecordsPerSec += s.ScanRecordsPerSec
			scanIndexesPerSec += s.ScanIndexesPerSec

			for _, p := range s.Plans {
				groupByPlanDigest[p.PlanDigest] = append(groupByPlanDigest[p.PlanDigest], p)
			}
		}

		tsql.CpuTimeSec = divideFloatAndFormat(cpuTimeMs, 1000)
		tsql.ExecCountPerSec = execCountPerSec
		// the average of the single execution time of the same SQL fingerprint on different nodes
		tsql.LatencyPerExecSec = divideFloatAndFormat(durationPerExecMs, 1000*len(sqls))
		tsql.ScanRecordsPerSec = scanRecordsPerSec
		tsql.ScanIndexesPerSec = scanIndexesPerSec
		tsql.PlanCounts = len(groupByPlanDigest)
		tsql.SqlDigest = sqls[0].SqlDigest
		tsql.SqlText = sqls[0].SqlText

		tdSqls = append(tdSqls, tsql)
	}

	// order by cpu times desc
	if len(tdSqls) > 0 {
		sort.Slice(tdSqls, func(i, j int) bool {
			return tdSqls[i].CpuTimeSec > tdSqls[j].CpuTimeSec
		})
	}
	return tdSqls
}

func divideFloatAndFormat(x float64, y int) float64 {
	div := float64(x) / float64(y)
	result := math.Round(div*100) / 100
	return result
}
