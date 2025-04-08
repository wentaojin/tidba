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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"github.com/wentaojin/tidba/utils/request"
)

const (
	DefaultMemoryMsgType    = "MEMORY"
	DefaultPlanCacheMsgType = "PLANCACHE"
)

func TopsqlMemoryUsage(ctx context.Context, clusterName string, nearly int, start, end string, top int, enableHistory bool, enableSqlDisplay bool) ([]*QueriedRespMsg, error) {
	var queriedRespMsgs []*QueriedRespMsg

	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return nil, err
	}
	db := connDB.(*mysql.Database)

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
	queries, err := GenerateTopsqlMemoryQuery(nearly, top, start, end, enableHistory, totalLatency)
	if err != nil {
		return nil, err
	}

	cols, res, err := db.GeneralQuery(ctx, queries)
	if err != nil {
		return nil, err
	}

	columns := []string{"Memory Size(MB)", "Executions", "Mem per Exec(MB)", "Latency per Exec(s)", "Min query Time(s)", "Max query Time(s)", `% Total SQL Time`, "SQL Digest"}

	if enableSqlDisplay {
		columns = append(columns, "SQL Text")
	}

	var rows [][]interface{}
	for _, r := range res {
		// exclude sort column name
		var row []interface{}
		for ind, c := range cols[1:] {
			for k, v := range r {
				if c == k {
					// total sql time percent
					if ind == 6 {
						float, err := decimal.NewFromString(v)
						if err != nil {
							return nil, err
						}
						row = append(row, fmt.Sprintf("%v%%", float.Mul(decimal.NewFromInt(100))))
					} else if ind == len(cols[1:])-1 {
						if enableSqlDisplay {
							row = append(row, v)
						}
					} else {
						row = append(row, v)
					}
				}
			}
		}
		rows = append(rows, row)
	}

	queriedRespMsgs = append(queriedRespMsgs, &QueriedRespMsg{
		MsgType: DefaultMemoryMsgType,
		Columns: columns,
		Results: rows,
	})

	topo, err := operator.GetDeployedClusterTopology(clusterName)
	if err != nil {
		return nil, err
	}
	planCacheS, err := TopsqlPlanCacheUsageSummary(ctx, topo, db, nearly, start, end)
	if err != nil {
		return nil, err
	}
	if len(planCacheS) > 0 {
		var instances []string

		// columns := []string{"Instance", "Avg Heap Usage(MB)", "Avg Plan Cache Usage(MB)", "Plan Cache Percentage(%)", "Plan Cache Mem Trend", "Plan Cache Max Size(MB)"}

		// var rows [][]interface{}
		for _, p := range planCacheS {
			instances = append(instances, fmt.Sprintf("'%s'", p.Instance))
			// var row []interface{}
			// row = append(row, p.Instance)
			// row = append(row, p.AvgHeapUsage)
			// row = append(row, p.AvgPlanCacheUsage)
			// row = append(row, p.PlanCachePercentage)
			// row = append(row, p.PlanCacheMemoryTrend)
			// row = append(row, p.InstPlanCacheMaxSize)
			// rows = append(rows, row)
		}
		// queriedRespMsgs = append(queriedRespMsgs, &QueriedRespMsg{
		// 	Columns: columns,
		// 	Results: rows,
		// })

		queries, err = GenerateTopsqlMemoryExecsQuery(nearly, top, start, end, enableHistory, totalLatency, instances)
		if err != nil {
			return nil, err
		}

		cols, res, err = db.GeneralQuery(ctx, queries)
		if err != nil {
			return nil, err
		}

		columns := []string{"Executions", "Elap per Exec(s)", "Parse Per Exec(s)", "Compile Per Exec(s)", "Memory Per Exec(MB)", "Min query Time(s)", "Max query Time(s)", `% Total SQL Time`, "SQL Digest"}

		if enableSqlDisplay {
			columns = append(columns, "SQL Text")
		}

		var newRows [][]interface{}
		for _, r := range res {
			// exclude sort column name
			var row []interface{}
			for ind, c := range cols[1:] {
				for k, v := range r {
					if c == k {
						// total sql time percent
						if ind == 7 {
							float, err := decimal.NewFromString(v)
							if err != nil {
								return nil, err
							}
							row = append(row, fmt.Sprintf("%v%%", float.Mul(decimal.NewFromInt(100))))
						} else if ind == len(cols[1:])-1 {
							if enableSqlDisplay {
								row = append(row, v)
							}
						} else {
							row = append(row, v)
						}
					}
				}
			}
			newRows = append(newRows, row)
		}

		queriedRespMsgs = append(queriedRespMsgs, &QueriedRespMsg{
			MsgType: DefaultPlanCacheMsgType,
			Columns: columns,
			Results: newRows,
		})
	}

	return queriedRespMsgs, nil
}

type PlanCacheSummary struct {
	Instance             string
	AvgHeapUsage         string
	AvgPlanCacheUsage    string
	PlanCachePercentage  string
	PlanCacheMemoryTrend string
	InstPlanCacheMaxSize string
}

// The memory usage of the execution plan shows an overall fluctuation/increase trend within the time window. Record the corresponding instance and current information
func TopsqlPlanCacheUsageSummary(ctx context.Context, topo *operator.ClusterTopology, db *mysql.Database, nearly int, start, end string) ([]PlanCacheSummary, error) {
	startTs, endTs, err := GenerateTimestampTSO(nearly, start, end)
	if err != nil {
		return nil, err
	}

	_, res, err := db.GeneralQuery(ctx, `show variables like 'tidb_enable_instance_plan_cache'`)
	if err != nil {
		return nil, err
	}

	var (
		api               string
		instPlanCacheSize string
	)
	api, err = GenPrometheusAPIPrefix(topo, `go_memory_classes_heap_objects_bytes{job="tidb"} + go_memory_classes_heap_unused_bytes{job="tidb"}`, startTs, endTs)
	if err != nil {
		return nil, err
	}

	resp, err := request.Request(request.DefaultRequestMethodGet, api, nil, "", "")
	if err != nil {
		return nil, err
	}

	instMemoryUsage, err := GetPromRequestAvgValueByMetric(api, resp)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		api, err = GenPrometheusAPIPrefix(topo, `tidb_server_plan_cache_instance_memory_usage{}`, startTs, endTs)
		if err != nil {
			return nil, err
		}
		instPlanCacheSize = "NULL"
	} else if res[0]["Value"] == "OFF" {
		api, err = GenPrometheusAPIPrefix(topo, `tidb_server_plan_cache_instance_memory_usage{type=" session-plan-cache"}`, startTs, endTs)
		if err != nil {
			return nil, err
		}
		instPlanCacheSize = "NULL"
	} else {
		_, res, err = db.GeneralQuery(ctx, `show variables like 'tidb_instance_plan_cache_max_size'`)
		if err != nil {
			return nil, err
		}
		vals, err := strconv.ParseInt(res[0]["Value"], 10, 64)
		if err != nil {
			return nil, err
		}

		instPlanCacheSize = fmt.Sprintf("%v", vals/1024/1024)

		api, err = GenPrometheusAPIPrefix(topo, `tidb_server_plan_cache_instance_memory_usage{type=" instance-plan-cache"}`, startTs, endTs)
		if err != nil {
			return nil, err
		}
	}
	resp, err = request.Request(request.DefaultRequestMethodGet, api, nil, "", "")
	if err != nil {
		return nil, err
	}

	planCacheMemoryUsage, err := GetPromRequestAvgValueByMetric(api, resp)
	if err != nil {
		return nil, err
	}
	memoryTrends, err := GetPromRequestValueMemoryTrend(api, resp)
	if err != nil {
		return nil, err
	}

	var pcp []PlanCacheSummary

	for p, pmUsage := range planCacheMemoryUsage {
		trens := memoryTrends[p]
		if trens == DefaultPlanCacheStatusFluctuation || trens == DefaultPlanCacheStatusVolatilityRises || trens == DefaultPlanCacheStatusGraduallyRising {
			if instUsage, ok := instMemoryUsage[p]; ok {
				percentage := pmUsage.Div(instUsage)
				pcp = append(pcp, PlanCacheSummary{
					Instance:             p,
					AvgHeapUsage:         fmt.Sprintf("%v", instUsage.Div(decimal.NewFromInt(1024*1024)).Round(2).String()),
					AvgPlanCacheUsage:    fmt.Sprintf("%v", pmUsage.Div(decimal.NewFromInt(1024*1024)).Round(2).String()),
					PlanCachePercentage:  fmt.Sprintf("%v%%", percentage.Round(4).Mul(decimal.NewFromInt(100)).String()),
					PlanCacheMemoryTrend: memoryTrends[p],
					InstPlanCacheMaxSize: instPlanCacheSize,
				})
			} else {
				return nil, fmt.Errorf("the corresponding plan cache instance [%s] cannot be found in the entire cluster [%s] tidb component instance", p, topo.ClusterMeta.ClusterName)
			}
		}
	}

	return pcp, nil
}

func GenerateTopsqlMemoryQuery(nearly, top int, start, end string, enableHistory bool, totalLatency string) (string, error) {
	var bs strings.Builder
	bs.WriteString(fmt.Sprintf(`/*+ monitoring */ select DENSE_RANK() OVER w AS 'total_memory_rank', aaa.*
from (SELECT
	ROUND(SUM(avg_mem*exec_count)/ 1024 / 1024,2) "total_memory_mb",
	SUM(exec_count) "total_execs",
    ROUND(AVG(avg_mem) / 1024 / 1024,4) "avg_memory_mb",
    AVG(avg_latency) / 1000000000 "avg_latency_s",
    MIN(min_latency) / 1000000000 "min_latency_s",
    MAX(max_latency) / 1000000000 "max_latency_s",
    ROUND(SUM(sum_latency) / 1000000000 / %v,4) "percentage",
    DIGEST "sql_digest",
    MAX(QUERY_SAMPLE_TEXT) "sql_text"`, totalLatency) + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'`, end, start) + "\n")
	}

	bs.WriteString(`AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'` + "\n")

	bs.WriteString(fmt.Sprintf(`GROUP BY
    SAMPLE_USER,
    DIGEST,
    SCHEMA_NAME) aaa WINDOW w AS(
ORDER BY total_memory_mb desc) LIMIT %d`, top))

	return bs.String(), nil
}

func GenerateTopsqlMemoryExecsQuery(nearly, top int, start, end string, enableHistory bool, totalLatency string, instances []string) (string, error) {
	var bs strings.Builder

	bs.WriteString(fmt.Sprintf(`/*+ monitoring */ select DENSE_RANK() OVER w AS 'total_execs_rank', aaa.*
from (SELECT
    SUM(exec_count) "total_execs",
    AVG(avg_latency) / 1000000000 "avg_latency_s",
    AVG(avg_parse_latency) / 1000000000 "avg_parse_latency_s",
    AVG(avg_compile_latency) / 1000000000 "avg_compile_latency_s",
	ROUND(AVG(avg_mem) / 1024 / 1024,4) "avg_memory_mb",
    MIN(min_latency) / 1000000000 "min_latency_s",
    MAX(max_latency) / 1000000000 "max_latency_s",
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
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'`, end, start) + "\n")
	}
	if len(instances) > 0 {
		bs.WriteString(fmt.Sprintf("AND a.INSTANCE IN (%s)\n", strings.Join(instances, ",")))
		bs.WriteString("AND (UPPER(a.QUERY_SAMPLE_TEXT) LIKE '%%WHERE%%IN%%' OR UPPER(a.QUERY_SAMPLE_TEXT) LIKE '%%INSERT%%INTO%%' OR UPPER(a.QUERY_SAMPLE_TEXT) LIKE '%%REPLACE%%INTO%%')\n")
	}
	bs.WriteString("AND a.STMT_TYPE IN ('Select','Insert','Replace)\n")
	bs.WriteString(`AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'` + "\n")

	bs.WriteString(fmt.Sprintf(`GROUP BY
    SAMPLE_USER,
    DIGEST,
    SCHEMA_NAME) aaa WINDOW w AS(
ORDER BY total_execs desc) limit %d`, top))

	return bs.String(), nil
}

func GenPrometheusAPIPrefix(topo *operator.ClusterTopology, qpsQuery string, startTs, endTs int64) (string, error) {
	insts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNamePrometheus)
	if err != nil {
		return "", err
	}
	baseURL := fmt.Sprintf("http://%s:%d/api/v1/query_range", insts[0].Host, insts[0].Port)

	// Prepare query parameters
	params := url.Values{}
	params.Add("query", qpsQuery)

	startTimeStr := strconv.FormatInt(startTs, 10)
	endTimeStr := strconv.FormatInt(endTs, 10)

	params.Add("start", startTimeStr)
	params.Add("end", endTimeStr)
	params.Add("step", "30s")

	return fmt.Sprintf("%s?%s", baseURL, params.Encode()), nil
}

type PromResp struct {
	Status string   `json:"status"`
	Data   RespData `json:"data"`
}

type RespData struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

type Result struct {
	Metric interface{}     `json:"metric"`
	Values [][]interface{} `json:"values"`
}

func GetPromRequestAvgValueByMetric(req string, resp []byte, pdRegionReq ...bool) (map[string]decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return nil, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	instAvg := make(map[string]decimal.Decimal)
	for _, res := range promResp.Data.Result {
		totalVal := decimal.NewFromInt(0)

		metric := res.Metric.(map[string]interface{})

		valNums := len(res.Values)
		for _, r := range res.Values {
			var valStr string
			if r[1] == nil || r[1].(string) == "NaN" {
				valStr = "0"
			} else {
				valStr = r[1].(string)
			}
			val, err := decimal.NewFromString(valStr)
			if err != nil {
				return nil, fmt.Errorf("parse uint value [%s] failed: %v", r[1], err)
			}
			totalVal = totalVal.Add(val)
		}
		if len(pdRegionReq) > 0 {
			instAvg[metric["address"].(string)] = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
		} else {
			instAvg[metric["instance"].(string)] = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
		}
	}
	return instAvg, nil
}

func GetPromRequestValueMemoryTrend(req string, resp []byte) (map[string]string, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return nil, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	instTrends := make(map[string]string)
	for _, res := range promResp.Data.Result {
		metric := res.Metric.(map[string]interface{})

		var trends []float64
		for _, r := range res.Values {
			var valStr string
			if r[1] == nil || r[1].(string) == "NaN" {
				valStr = "0"
			} else {
				valStr = r[1].(string)
			}
			val, err := strconv.ParseUint(valStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse string value int[%s] failed: %v", r[1], err)
			}
			trends = append(trends, float64(val))
		}

		trs, err := determineTrend(trends)
		if err != nil {
			return nil, err
		}
		instTrends[metric["instance"].(string)] = trs
	}

	return instTrends, nil
}

// 线性回归计算斜率和截距
func linearRegression(x, y []float64) (float64, float64, error) {
	var slope, intercept float64
	n := len(x)
	if n != len(y) || n == 0 {
		return 0, 0, fmt.Errorf("x and y must have the same length and not be empty")
	}

	var sumX, sumY, sumXY, sumX2 float64
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
	}

	slope = (float64(n)*sumXY - sumX*sumY) / (float64(n)*sumX2 - sumX*sumX)
	intercept = (sumY - slope*sumX) / float64(n)
	return slope, intercept, nil
}

// 计算标准差
func standardDeviation(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}

	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	variance := 0.0
	for _, v := range values {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(n)

	return math.Sqrt(variance)
}

// 判断趋势的函数
func determineTrend(values []float64) (string, error) {
	if len(values) < 2 {
		return DefaultPlanCacheStatusUnknown, nil
	}

	// 构造 x 轴（时间轴）
	x := make([]float64, len(values))
	for i := range x {
		x[i] = float64(i)
	}

	// 线性回归计算斜率和截距
	slope, _, err := linearRegression(x, values)
	if err != nil {
		return "", err
	}
	// 计算标准差，衡量波动性
	stdDev := standardDeviation(values)

	// 根据斜率和波动性判断趋势
	if math.Abs(slope) < 1e-6 { // 斜率接近零
		if stdDev < 1e-6 {
			return DefaultPlanCacheStatusSmooth, nil
		}
		return DefaultPlanCacheStatusFluctuation, nil
	} else if slope > 0 { // 斜率为正
		if stdDev > 1e-6 {
			return DefaultPlanCacheStatusVolatilityRises, nil
		}
		return DefaultPlanCacheStatusGraduallyRising, nil
	} else { // 斜率为负
		if stdDev > 1e-6 {
			return DefaultPlanCacheStatusVolatilityDecreases, nil
		}
		return DefaultPlanCacheStatusGraduallyDecreases, nil
	}
}

const (
	DefaultPlanCacheStatusUnknown             = "unknown"
	DefaultPlanCacheStatusSmooth              = "smooth"
	DefaultPlanCacheStatusFluctuation         = "fluctuation"
	DefaultPlanCacheStatusVolatilityRises     = "volatility rises"
	DefaultPlanCacheStatusVolatilityDecreases = "volatility decreases"
	DefaultPlanCacheStatusGraduallyRising     = "gradually rising"
	DefaultPlanCacheStatusGraduallyDecreases  = "gradually decreases"
)

var TrendStringRule = map[string]string{
	DefaultPlanCacheStatusSmooth:              "整体呈平稳状态",
	DefaultPlanCacheStatusFluctuation:         "整体呈波动趋势",
	DefaultPlanCacheStatusVolatilityRises:     "整体呈波动上升趋势",
	DefaultPlanCacheStatusVolatilityDecreases: "整体呈波动下降趋势",
	DefaultPlanCacheStatusGraduallyRising:     "整体呈逐渐上涨趋势",
	DefaultPlanCacheStatusGraduallyDecreases:  "整体呈逐渐下降趋势",
	DefaultPlanCacheStatusUnknown:             "数据不超过 2，无法判断趋势",
}
