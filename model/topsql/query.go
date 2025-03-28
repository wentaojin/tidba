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
	"sort"
	"strings"
	"sync"
	"time"

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
	ScanRecordsPerSec int     `json:"scan_records_per_sec"`
	ScanIndexesPerSec int     `json:"scan_indexes_per_sec"`
	Plans             []*Plan `json:"plans"`
}

type Plan struct {
	PlanDigest        string  `json:"plan_digest"`
	PlanText          string  `json:"plan_text"`
	TimestampSec      []int   `json:"timestamp_sec"`
	CpuTimeMs         []int   `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	DurationPerExecMs float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec int     `json:"scan_records_per_sec"`
	ScanIndexesPerSec int     `json:"scan_indexes_per_sec"`
}

type CPU struct {
	CpuTimeSec           float64
	ExecCountsPerSec     float64
	LatencyPerExecSec    float64
	ScanRecordPerSec     int
	ScanIndexesPerSec    int
	PlanDigestCounts     int
	MaxPlanSqlLatencySec float64
	MinPlanSqlLatencySec float64
	SqlLatencyPercent    string
	SqlDigest            string
	SqlText              string
}

type NewPlan struct {
	PlanDigest        string  `json:"plan_digest"`
	PlanText          string  `json:"plan_text"`
	TimestampSec      []int   `json:"timestamp_sec"`
	CpuTimeSec        float64 `json:"cpu_time_ms"`
	ExecCountPerSec   float64 `json:"exec_count_per_sec"`
	LatencyPerExecMs  float64 `json:"duration_per_exec_ms"`
	ScanRecordsPerSec int     `json:"scan_records_per_sec"`
	ScanIndexesPerSec int     `json:"scan_indexes_per_sec"`
}

func GenerateTosqlCpuTimeByComponentServer(ctx context.Context, clusterName, component string, nearly, top int, start, end string, concurrency int, instances []string) ([]*CPU, error) {
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
			return nil, fmt.Errorf("the component [%s] instances [%] not founds, please review and reconfigure", component, differs)
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

	var cpus []*CPU
	for digest, sqls := range groupbySqlDigest {
		if digest == "" {
			continue
		}
		counts := len(sqls)

		cpuTimeMs := 0
		execPerSec := 0.0
		latencyPerMs := 0.0
		scanRecordsPerSec := 0
		scanIndexesPerSec := 0

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
			scanRecordsPerSec := 0
			scanIndexesPerSec := 0
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

		cpus = append(cpus, &CPU{
			CpuTimeSec:           math.Round(float64(cpuTimeMs)/1000*100) / 100,
			ExecCountsPerSec:     math.Round(execPerSec*100) / 100,
			LatencyPerExecSec:    math.Round(latencyPerMs/1000/float64(counts)*100) / 100,
			ScanRecordPerSec:     scanRecordsPerSec,
			ScanIndexesPerSec:    scanIndexesPerSec,
			PlanDigestCounts:     planUniqCounts,
			SqlDigest:            digest,
			SqlText:              sqls[0].SqlText,
			SqlLatencyPercent:    fmt.Sprintf("%v%%", math.Round(float64(cpuTimeMs)/float64(sqlTotalLatency)*100)),
			MaxPlanSqlLatencySec: math.Round(newPlans[len(newPlans)-1].LatencyPerExecMs/1000*100) / 100,
			MinPlanSqlLatencySec: math.Round(newPlans[0].LatencyPerExecMs/1000*100) / 100,
		})
	}

	sort.Slice(cpus, func(i, j int) bool {
		return cpus[i].CpuTimeSec <= cpus[j].CpuTimeSec
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
