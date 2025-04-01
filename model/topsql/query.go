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
package topsql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"github.com/wentaojin/tidba/utils/request"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

func GenerateQueryWindowSqlElapsedTime(nearly int, start, end string, enableHistory bool) (string, error) {
	var bs strings.Builder
	bs.WriteString(`select 
     SUM(sum_latency) / 1000000000 as "all_latency_s"` + "\n")

	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly))
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start))
	}

	return bs.String(), nil
}

func GenerateTopsqlElapsedTimeQuery(nearly, top int, start, end string, enableHistory bool, totalLatency string) (string, error) {
	var bs strings.Builder
	bs.WriteString(fmt.Sprintf(`/*+ monitoring */ select DENSE_RANK() OVER w AS 'total_latency_rank', aaa.*
  from (SELECT 
               SUM(sum_latency) / 1000000000 "total_latency_s",
               SUM(exec_count) "total_execs",
               AVG(avg_latency) / 1000000000 "avg_latency_s",
               MIN(min_latency) / 1000000000 "min_latency_s",
               MAX(max_latency) / 1000000000 "max_latency_s",
               AVG(AVG_TOTAL_KEYS) "avg_total_keys",
               AVG(AVG_PROCESSED_KEYS) "avg_processed_keys",
               ROUND(SUM(sum_latency) / 1000000000 / %v,2) "percentage",
               DIGEST "sql_digest",
               MIN(QUERY_SAMPLE_TEXT) "sql_text"`, totalLatency) + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
	}

	bs.WriteString(fmt.Sprintf("GROUP BY SAMPLE_USER, DIGEST, SCHEMA_NAME) aaa WINDOW w AS(ORDER BY total_latency_s desc) limit %d", top))

	return bs.String(), nil
}

func GenerateTopsqlExecutionsQuery(nearly, top int, start, end string, enableHistory bool, totalLatency string) (string, error) {
	var bs strings.Builder
	bs.WriteString(fmt.Sprintf(`/*+ monitoring */ select DENSE_RANK() OVER w AS 'total_execs_rank', aaa.*
from (SELECT
    SUM(exec_count) "total_execs",
    AVG(avg_latency) / 1000000000 "avg_latency_s",
    AVG(avg_parse_latency) / 1000000000 "avg_parse_latency_s",
    AVG(avg_compile_latency) / 1000000000 "avg_compile_latency_s",
    MIN(min_latency) / 1000000000 "min_latency_s",
    MAX(max_latency) / 1000000000 "max_latency_s",
    AVG(AVG_TOTAL_KEYS) "avg_total_keys",
    AVG(AVG_PROCESSED_KEYS) "avg_processed_keys",
    ROUND(SUM(sum_latency) / 1000000000 / %v,2) "percentage",
    DIGEST "sql_digest",
    MIN(QUERY_SAMPLE_TEXT) "sql_text"`, totalLatency) + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
	}

	bs.WriteString(fmt.Sprintf(`GROUP BY
    SAMPLE_USER,
    DIGEST,
    SCHEMA_NAME) aaa WINDOW w AS(
ORDER BY total_execs desc) limit %d`, top))

	return bs.String(), nil
}

func GenerateTopsqlPlansQuery(nearly, top int, start, end string, enableHistory bool, totalLatency string) (string, error) {
	var bs strings.Builder

	bs.WriteString(`/*+ monitoring */ select DENSE_RANK() OVER w AS 'plan_counts_rank', aaa.*
from (SELECT
    COUNT(DISTINCT plan_digest) as plan_digest_counts,
    SUM(sum_latency) / 1000000000 as total_latency_s,
    SUM(exec_count) as total_execs,
    CONCAT(MIN(min_latency) / 1000000000,' , ',(
        SELECT plan_digest` + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history sub_min` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary sub_min` + "\n")
	}
	bs.WriteString(`        WHERE sub_min.DIGEST = a.DIGEST
                AND sub_min.sample_user = a.SAMPLE_USER
                AND sub_min.schema_name = a.SCHEMA_NAME` + "\n")
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`AND sub_min.summary_begin_time <= NOW()
            AND sub_min.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
            AND sub_min.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
        ORDER BY min_latency ASC
        LIMIT 1
    )) as plan_digest_for_min_latency,`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`AND sub_min.summary_begin_time <= '%s'
		AND sub_min.summary_end_time >= '%s'
		AND sub_min.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
	ORDER BY min_latency ASC
	LIMIT 1
)) as plan_digest_for_min_latency,`, end, start) + "\n")
	}

	bs.WriteString(` CONCAT(MAX(max_latency) / 1000000000,' , ',(
        SELECT plan_digest` + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history sub_max` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary sub_max` + "\n")
	}

	bs.WriteString(`WHERE sub_max.DIGEST = a.DIGEST
                AND sub_max.sample_user = a.SAMPLE_USER
                AND sub_max.schema_name = a.SCHEMA_NAME` + "\n")

	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(` AND sub_max.summary_begin_time <= NOW()
            AND sub_max.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
            AND sub_max.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
        ORDER BY max_latency DESC
        LIMIT 1
    )) as plan_digest_for_max_latency,`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(` AND sub_max.summary_begin_time <= '%s'
            AND sub_max.summary_end_time >= '%s'
            AND sub_max.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
        ORDER BY max_latency DESC
        LIMIT 1
    )) as plan_digest_for_max_latency,`, end, start) + "\n")
	}

	bs.WriteString(`AVG(AVG_TOTAL_KEYS) as avg_total_keys,
    AVG(AVG_PROCESSED_KEYS) as avg_processed_keys,` + "\n")
	bs.WriteString(fmt.Sprintf(`ROUND(SUM(sum_latency) / 1000000000 / %v,2) "percentage",`, totalLatency) + "\n")
	bs.WriteString(`DIGEST as sql_digest,
    MIN(QUERY_SAMPLE_TEXT) as sql_text` + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}

	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
	}

	bs.WriteString(fmt.Sprintf(`GROUP BY
    SAMPLE_USER,
    DIGEST,
    SCHEMA_NAME
HAVING COUNT(DISTINCT plan_digest) > 1)  aaa WINDOW w AS(
ORDER BY plan_digest_counts desc) limit %d`, top))

	return bs.String(), nil
}

type Data struct {
	Mutex *sync.Mutex `json:"-"`
	Data  []*Sql      `json:"data"`
}

func (d *Data) Append(sqls []*Sql) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	d.Data = append(d.Data, sqls...)
}

type Sql struct {
	SqlDigest         string  `json:"sql_digest"`
	SqlText           string  `json:"sql_text"`
	IsOther           bool    `json:"is_other"`
	CpuTimeMs         int     `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	DurationPerExecMs float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec float64 `json:"scan_records_per_sec"`
	ScanIndexesPerSec float64 `json:"scan_indexes_per_sec"`
	Plans             []*Plan `json:"plans"`
}

type Plan struct {
	PlanDigest        string  `json:"plan_digest"`
	PlanText          string  `json:"plan_text"`
	TimestampSec      []int   `json:"timestamp_sec"`
	CpuTimeMs         []int   `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	DurationPerExecMs float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec float64 `json:"scan_records_per_sec"`
	ScanIndexesPerSec float64 `json:"scan_indexes_per_sec"`
}

type CPU struct {
	CpuTimeSec           float64 `json:"cpu_time_sec"`
	ExecCountsPerSec     float64 `json:"exec_counts_per_sec"`
	LatencyPerExecSec    float64 `json:"latency_per_exec_sec"`
	ScanRecordPerSec     float64 `json:"scan_record_per_sec"`
	ScanIndexesPerSec    float64 `json:"scan_indexes_per_sec"`
	PlanDigestCounts     int     `json:"plan_digest_counts"`
	MaxPlanSqlLatencySec float64 `json:"max_plan_latency_sec"`
	MinPlanSqlLatencySec float64 `json:"min_plan_latency_sec"`
	SqlLatencyPercent    float64 `json:"percentage"`
	SqlDigest            string  `json:"-"`
	SqlText              string  `json:"-"`
}

type NewPlan struct {
	PlanDigest        string  `json:"plan_digest"`
	PlanText          string  `json:"plan_text"`
	TimestampSec      []int   `json:"timestamp_sec"`
	CpuTimeSec        float64 `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	LatencyPerExecMs  float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec float64 `json:"scan_records_per_sec"`
	ScanIndexesPerSec float64 `json:"scan_indexes_per_sec"`
}

func GenerateTosqlCpuTimeByComponentServer(ctx context.Context, clusterName, component string, nearly, top int, start, end string, concurrency int, instances []string) ([]CPU, error) {
	startSecs, endSecs, err := GenerateTimestampTSO(nearly, start, end)
	if err != nil {
		return nil, err
	}

	topo, err := operator.GetDeployedClusterTopology(clusterName)
	if err != nil {
		return nil, err
	}

	var (
		totalAddrs []string
		ngAddr     string
		addrs      []string
	)
	if strings.EqualFold(component, operator.ComponentNameTiDB) {
		totalAddrs, _, ngAddr = topo.GetClusterComponentStatusPortByTopSqlCPU()
	} else if strings.EqualFold(component, operator.ComponentNameTiKV) {
		_, totalAddrs, ngAddr = topo.GetClusterComponentStatusPortByTopSqlCPU()
	} else {
		return nil, fmt.Errorf("unknown component [%s], only support options for tidb / tikv", component)
	}

	if len(instances) > 0 {
		differs := stringutil.CompareSliceString(instances, totalAddrs)
		if len(differs) > 0 {
			return nil, fmt.Errorf("the component [%s] instances [%v] not founds, please review and reconfigure", component, differs)
		}
		addrs = instances
	} else {
		addrs = totalAddrs
	}

	datas := &Data{
		Mutex: &sync.Mutex{},
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, tidb := range addrs {
		addr := tidb
		g.Go(func() error {
			if err := request.Retry(
				&request.RetryConfig{
					MaxRetries: request.DefaultRequestErrorMaxRetries,
					Delay:      request.DefaultRequestErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					url := generateTopsqlRequestAPI(ngAddr, startSecs, endSecs, addr, component, top)

					resp, err := Request("GET", url, nil)
					if err != nil {
						return err
					}

					var data *Data
					if err := json.Unmarshal(resp, &data); err != nil {
						return err
					}
					datas.Append(data.Data)
					return nil
				},
			); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	sqlTotalLatency := 0
	groupbySqlDigest := make(map[string][]*Sql)
	for _, d := range datas.Data {
		if vals, ok := groupbySqlDigest[d.SqlDigest]; ok {
			vals = append(vals, d)
			groupbySqlDigest[d.SqlDigest] = vals
		} else {
			var sqls []*Sql
			sqls = append(sqls, d)
			groupbySqlDigest[d.SqlDigest] = sqls
		}
		sqlTotalLatency = sqlTotalLatency + d.CpuTimeMs
	}

	var cpus []CPU
	for digest, sqls := range groupbySqlDigest {
		if digest == "" {
			continue
		}
		counts := len(sqls)

		cpuTimeMs := 0
		execPerSec := 0.0
		latencyPerMs := 0.0
		scanRecordsPerSec := 0.0
		scanIndexesPerSec := 0.0

		groupbyPlanDigest := make(map[string][]*Plan)

		for _, s := range sqls {
			cpuTimeMs = cpuTimeMs + s.CpuTimeMs
			execPerSec = execPerSec + s.ExecCountPerSec
			latencyPerMs = latencyPerMs + s.DurationPerExecMs
			scanRecordsPerSec = scanRecordsPerSec + s.ScanRecordsPerSec
			scanIndexesPerSec = scanIndexesPerSec + s.ScanIndexesPerSec

			for _, p := range s.Plans {
				if vals, ok := groupbyPlanDigest[p.PlanDigest]; ok {
					vals = append(vals, p)
					groupbyPlanDigest[p.PlanDigest] = vals
				} else {
					var plans []*Plan
					plans = append(plans, p)
					groupbyPlanDigest[p.PlanDigest] = plans
				}
			}
		}

		planUniqCounts := len(groupbyPlanDigest)

		var newPlans []*NewPlan

		for planDigest, plans := range groupbyPlanDigest {
			cpuTimeMs := 0
			execPerSec := 0.0
			totalLatencyPerMs := 0.0
			scanRecordsPerSec := 0.0
			scanIndexesPerSec := 0.0
			var (
				planText     string
				timestampSec []int
			)

			plansCounts := len(plans)
			for _, p := range plans {
				planText = p.PlanText
				timestampSec = p.TimestampSec

				totalCpuTimeMs := 0
				for _, ms := range p.CpuTimeMs {
					totalCpuTimeMs = totalCpuTimeMs + ms
				}
				cpuTimeMs = cpuTimeMs + totalCpuTimeMs

				execPerSec = execPerSec + p.ExecCountPerSec
				totalLatencyPerMs = totalLatencyPerMs + p.DurationPerExecMs
				scanRecordsPerSec = scanRecordsPerSec + p.ScanRecordsPerSec
				scanIndexesPerSec = scanIndexesPerSec + p.ScanIndexesPerSec
			}

			newPlans = append(newPlans, &NewPlan{
				PlanDigest:        planDigest,
				PlanText:          planText,
				TimestampSec:      timestampSec,
				CpuTimeSec:        math.Round(float64(cpuTimeMs)/1000*100) / 100,
				ExecCountPerSec:   math.Round(execPerSec*100) / 100,
				LatencyPerExecMs:  math.Round(totalLatencyPerMs/float64(plansCounts)*100) / 100,
				ScanRecordsPerSec: scanRecordsPerSec,
				ScanIndexesPerSec: scanIndexesPerSec,
			})
		}

		sort.Slice(newPlans, func(i, j int) bool {
			return newPlans[i].CpuTimeSec <= newPlans[j].CpuTimeSec
		})

		cpus = append(cpus, CPU{
			CpuTimeSec:           math.Round(float64(cpuTimeMs)/1000*100) / 100,
			ExecCountsPerSec:     math.Round(execPerSec*100) / 100,
			LatencyPerExecSec:    math.Round(latencyPerMs/1000/float64(counts)*100) / 100,
			ScanRecordPerSec:     math.Round(scanRecordsPerSec*100) / 100,
			ScanIndexesPerSec:    math.Round(scanIndexesPerSec*100) / 100,
			PlanDigestCounts:     planUniqCounts,
			SqlDigest:            digest,
			SqlText:              sqls[0].SqlText,
			SqlLatencyPercent:    math.Round(float64(cpuTimeMs) / float64(sqlTotalLatency)),
			MaxPlanSqlLatencySec: math.Round(newPlans[len(newPlans)-1].LatencyPerExecMs/1000*100) / 100,
			MinPlanSqlLatencySec: math.Round(newPlans[0].LatencyPerExecMs/1000*100) / 100,
		})
	}

	sort.Slice(cpus, func(i, j int) bool {
		return cpus[i].CpuTimeSec >= cpus[j].CpuTimeSec
	})

	return cpus, nil
}

func generateTopsqlRequestAPI(ngAddr string, startSec, endSec int64, instAddr, compName string, top int) string {
	return fmt.Sprintf("http://%s/topsql/v1/summary?end=%d&instance=%s&instance_type=%s&start=%d&top=%d", ngAddr, endSec, instAddr, strings.ToLower(compName), startSec, top)
}

func GenerateTimestampTSO(nearly int, start, end string) (int64, int64, error) {
	if nearly > 0 {
		endTime := time.Now()
		startTime := endTime.Add(-time.Duration(nearly) * time.Minute)
		return startTime.Unix(), endTime.Unix(), nil
	}

	if start == "" || end == "" {
		return 0, 0, fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
	}

	layout := "2006-01-02 15:04:05"
	parsedStime, err := time.Parse(layout, start)
	if err != nil {
		return 0, 0, err
	}
	parsedEtime, err := time.Parse(layout, end)
	if err != nil {
		return 0, 0, err
	}
	return parsedStime.Unix(), parsedEtime.Unix(), nil
}

func Request(method, url string, body []byte) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}

/*
Scoring model for locating the top 5 SQLs that affect cluster performance:
1. Standardize the score of sql digest for each dimension (scores are ranked from high to low according to the top number, for example: top 10, highest 10 points, lowest 1 point)
2. Aggregate sql digest for each dimension and calculate the total score by weight
- TiKV CPU 35% (component instance aggregation)
- TiDB CPU 25% (component instance aggregation)
- Total time 20%
- Number of executions 15%
- Change in execution plan 5%
3. Sort by weighted total score and take the top. If the scores are the same, take the weighted priority (TiKV CPU -> TiDB CPU -> Total time -> Number of executions -> Execution plan)
*/

type Elapsed struct {
	TotalLatencyRank string `json:"rank"`
	TotalLatcncySec  string `json:"elapsed_sec"`
	TotalExecutions  string `json:"exec_counts"`
	AvgLatencySec    string `json:"avg_latency_sec"`
	MinLatencySec    string `json:"min_latency_sec"`
	MaxLatencySec    string `json:"max_latency_sec"`
	AvgTotalKeys     string `json:"avg_total_keys"`
	AvgProcessedKeys string `json:"avg_processed_keys"`
	Percentage       string `json:"percentage"`
	SqlDigest        string `json:"-"`
	SqlText          string `json:"-"`
	Score            int    `json:"score"`
}

func (e Elapsed) String() string {
	if reflect.DeepEqual(e, Elapsed{}) {
		return "NULL"
	}
	formattedJSON, _ := json.MarshalIndent(e, "", " ")
	return string(formattedJSON)
}

type Executions struct {
	TotalExecsRank       string `json:"rank"`
	TotalExecutions      string `json:"exec_counts"`
	AvgLatencySec        string `json:"avg_latency_sec"`
	AvgParseLatencySec   string `json:"avg_parse_latency_sec"`
	AvgCompileLatencySec string `json:"avg_compile_latency_sec"`
	MinLatencySec        string `json:"min_latency_sec"`
	MaxLatencySec        string `json:"max_latency_sec"`
	AvgTotalKeys         string `json:"avg_total_keys"`
	AvgProcessedKeys     string `json:"avg_processed_keys"`
	Percentage           string `json:"percentage"`
	SqlDigest            string `json:"-"`
	SqlText              string `json:"-"`
	Score                int    `json:"score"`
}

func (e Executions) String() string {
	if reflect.DeepEqual(e, Executions{}) {
		return "NULL"
	}
	formattedJSON, _ := json.MarshalIndent(e, "", " ")
	return string(formattedJSON)
}

type Plans struct {
	PlanCountsRank   string `json:"rank"`
	SqlPlans         string `json:"plan_counts"`
	ElapsedTimeSec   string `json:"elapsed_sec"`
	Executions       string `json:"exec_counts"`
	MinSqlPlanSec    string `json:"min_sql_plan_sec"`
	MaxSqlPlanSec    string `json:"max_sql_plan_sec"`
	AvgTotalKeys     string `json:"avg_total_keys"`
	AvgProcessedKeys string `json:"avg_processed_keys"`
	Percentage       string `json:"percentage"`
	SqlDigest        string `json:"-"`
	SqlText          string `json:"-"`
	Score            int    `json:"score"`
}

func (p Plans) String() string {
	if reflect.DeepEqual(p, Plans{}) {
		return "NULL"
	}
	formattedJSON, _ := json.MarshalIndent(p, "", " ")
	return string(formattedJSON)
}

type TiDBCpu struct {
	CPU
	Score int `json:"score"`
}

func (c TiDBCpu) String() string {
	if reflect.DeepEqual(c, TiDBCpu{}) {
		return "NULL"
	}
	formattedJSON, _ := json.MarshalIndent(c, "", " ")
	return string(formattedJSON)
}

type TiKVCpu struct {
	CPU
	Score int `json:"score"`
}

func (c TiKVCpu) String() string {
	if reflect.DeepEqual(c, TiKVCpu{}) {
		return "NULL"
	}
	formattedJSON, _ := json.MarshalIndent(c, "", " ")
	return string(formattedJSON)
}

type Diagnosis struct {
	Score     float64
	SqlDigest string
	TiKVCpu   TiKVCpu
	TiDBCpu   TiDBCpu
	Elapsed   Elapsed
	Execs     Executions
	Plans     Plans
	SqlText   string
}

func TopsqlDiagnosis(ctx context.Context, clusterName string, db *mysql.Database, nearly, top int, start, end string, concurrency int, enableHistory bool) ([][]interface{}, error) {
	totalLatencySql, err := GenerateQueryWindowSqlElapsedTime(nearly, start, end, enableHistory)
	if err != nil {
		return nil, err
	}

	_, res, err := db.GeneralQuery(ctx, totalLatencySql)
	if err != nil {
		return nil, err
	}
	var totalLatency string
	if len(res) == 0 {
		return nil, fmt.Errorf("the database sql [%v] query time windows result not found", totalLatencySql)
	} else {
		totalLatency = res[0]["all_latency_s"]
	}

	globalSqlDigest := make(map[string]struct{})

	elapsed, err := GenerateTopsqlElapsedTimeQuery(nearly, top, start, end, enableHistory, totalLatency)
	if err != nil {
		return nil, err
	}

	_, elapsedRes, err := db.GeneralQuery(ctx, elapsed)
	if err != nil {
		return nil, err
	}

	// standard score
	var elapseds []Elapsed
	for idx, r := range elapsedRes {
		elapseds = append(elapseds, Elapsed{
			TotalLatencyRank: r["total_latency_rank"],
			TotalLatcncySec:  r["total_latency_s"],
			TotalExecutions:  r["total_execs"],
			AvgLatencySec:    r["avg_latency_s"],
			MinLatencySec:    r["min_latency_s"],
			MaxLatencySec:    r["max_latency_s"],
			AvgTotalKeys:     r["avg_total_keys"],
			AvgProcessedKeys: r["avg_processed_keys"],
			Percentage:       r["percentage"],
			SqlDigest:        r["sql_digest"],
			SqlText:          r["sql_text"],
			Score:            top - idx,
		})
		globalSqlDigest[r["sql_digest"]] = struct{}{}
	}

	execsql, err := GenerateTopsqlExecutionsQuery(nearly, top, start, end, enableHistory, totalLatency)
	if err != nil {
		return nil, err
	}

	_, execRes, err := db.GeneralQuery(ctx, execsql)
	if err != nil {
		return nil, err
	}

	// standard score
	var executions []Executions
	for idx, r := range execRes {
		executions = append(executions, Executions{
			TotalExecsRank:       r["total_execs_rank"],
			TotalExecutions:      r["total_execs"],
			AvgLatencySec:        r["avg_latency_s"],
			AvgParseLatencySec:   r["avg_parse_latency_s"],
			AvgCompileLatencySec: r["avg_compile_latency_s"],
			MinLatencySec:        r["min_latency_s"],
			MaxLatencySec:        r["max_latency_s"],
			AvgTotalKeys:         r["avg_total_keys"],
			AvgProcessedKeys:     r["avg_processed_keys"],
			Percentage:           r["percentage"],
			SqlDigest:            r["sql_digest"],
			SqlText:              r["sql_text"],
			Score:                top - idx,
		})
		globalSqlDigest[r["sql_digest"]] = struct{}{}
	}

	plansql, err := GenerateTopsqlPlansQuery(nearly, top, start, end, enableHistory, totalLatency)
	if err != nil {
		return nil, err
	}

	_, planRes, err := db.GeneralQuery(ctx, plansql)
	if err != nil {
		return nil, err
	}

	// standard score
	var plans []Plans
	for idx, r := range planRes {
		plans = append(plans, Plans{
			PlanCountsRank:   r["plan_counts_rank"],
			SqlPlans:         r["plan_digest_counts"],
			ElapsedTimeSec:   r["total_latency_s"],
			Executions:       r["total_execs"],
			MinSqlPlanSec:    r["plan_digest_for_min_latency"],
			MaxSqlPlanSec:    r["plan_digest_for_max_latency"],
			AvgTotalKeys:     r["avg_total_keys"],
			AvgProcessedKeys: r["avg_processed_keys"],
			Percentage:       r["percentage"],
			SqlDigest:        r["sql_digest"],
			SqlText:          r["sql_text"],
			Score:            top - idx,
		})
		globalSqlDigest[r["sql_digest"]] = struct{}{}
	}

	cpus, err := GenerateTosqlCpuTimeByComponentServer(ctx, clusterName, operator.ComponentNameTiKV, nearly, top, start, end, concurrency, nil)
	if err != nil {
		return nil, err
	}

	var tikvCpus []TiKVCpu
	for idx, c := range cpus {
		tikvCpus = append(tikvCpus, TiKVCpu{
			CPU:   c,
			Score: top - idx,
		})
		globalSqlDigest[c.SqlDigest] = struct{}{}
	}

	cpus, err = GenerateTosqlCpuTimeByComponentServer(ctx, clusterName, operator.ComponentNameTiDB, nearly, top, start, end, concurrency, nil)
	if err != nil {
		return nil, err
	}

	var tidbCPus []TiDBCpu
	for idx, c := range cpus {
		tidbCPus = append(tidbCPus, TiDBCpu{
			CPU:   c,
			Score: top - idx,
		})
		globalSqlDigest[c.SqlDigest] = struct{}{}
	}

	// merge sql digest and compute score
	var diags []Diagnosis

	for d, _ := range globalSqlDigest {
		// exclude sql digest null
		if d == "" {
			continue
		}
		var diag Diagnosis
		score := 0.0
		diag.SqlDigest = d

		for _, e := range tikvCpus {
			if d == e.SqlDigest {
				score = score + float64(e.Score)*0.35
				diag.TiKVCpu = e
				diag.SqlText = e.SqlText
			}
		}

		for _, e := range tidbCPus {
			if d == e.SqlDigest {
				score = score + float64(e.Score)*0.25
				diag.TiDBCpu = e
				diag.SqlText = e.SqlText
			}
		}

		for _, e := range elapseds {
			if d == e.SqlDigest {
				score = score + float64(e.Score)*0.20
				diag.Elapsed = e
				diag.SqlText = e.SqlText
			}
		}

		for _, e := range executions {
			if d == e.SqlDigest {
				score = score + float64(e.Score)*0.15
				diag.Execs = e
				diag.SqlText = e.SqlText
			}
		}

		for _, e := range plans {
			if d == e.SqlDigest {
				score = score + float64(e.Score)*0.05
				diag.Plans = e
				diag.SqlText = e.SqlText
			}
		}
		diag.Score = math.Round(score*100) / 100
		diags = append(diags, diag)
	}

	sort.Slice(diags, func(i, j int) bool {
		// Sort by Score from largest to smallest
		if diags[i].Score != diags[j].Score {
			return diags[i].Score > diags[j].Score
		}

		// If the scores are the same, sort by TiKVCpu.Score
		if diags[i].TiKVCpu.Score != diags[j].TiKVCpu.Score {
			return diags[i].TiKVCpu.Score > diags[j].TiKVCpu.Score
		}

		// If TiKVCpu.Score is the same, sort by TiDBCpu.Score
		if diags[i].TiDBCpu.Score != diags[j].TiDBCpu.Score {
			return diags[i].TiDBCpu.Score > diags[j].TiDBCpu.Score
		}

		// If TiDBCpu.Score is the same, sort by Elapsed.Score
		if diags[i].Elapsed.Score != diags[j].Elapsed.Score {
			return diags[i].Elapsed.Score > diags[j].Elapsed.Score
		}

		// If Elapsed.Score is the same, sort by Execs.Score
		if diags[i].Execs.Score != diags[j].Execs.Score {
			return diags[i].Execs.Score > diags[j].Execs.Score
		}

		// If Execs.Score is the same, sort by Plans.Score
		return diags[i].Plans.Score > diags[j].Plans.Score
	})

	var rows [][]interface{}

	for _, d := range diags {
		var row []interface{}
		row = append(row, d.Score)
		row = append(row, d.SqlDigest)
		row = append(row, d.TiKVCpu.String())
		row = append(row, d.TiDBCpu.String())
		row = append(row, d.Elapsed.String())
		row = append(row, d.Execs.String())
		row = append(row, d.Plans.String())
		row = append(row, d.SqlText)
		rows = append(rows, row)
	}
	return rows, nil
}
