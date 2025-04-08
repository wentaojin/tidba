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
package main

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/tidba/utils/request"
)

func main() {
	// apis := api("120.92.108.85", "54321", `histogram_quantile(0.99, sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_bucket{}[1m])) by (address,store,le))`)

	apis := api("10.2.103.77", "9090", `tidb_server_plan_cache_instance_memory_usage{}`)

	maxResp, err := request.Request(request.DefaultRequestMethodGet, apis, nil, "", "")
	if err != nil {
		panic(err)
	}
	fmt.Println(get("x", maxResp))
}

func api(host string, port, qpsQuery string) string {
	baseURL := fmt.Sprintf("http://%s:%s/api/v1/query_range", host, port)

	// Prepare query parameters
	params := url.Values{}
	params.Add("query", qpsQuery)

	currentTime := time.Now().UTC()
	startTime := currentTime.Add(-time.Duration(3600/60) * time.Hour)
	startTimeStr := strconv.FormatInt(startTime.Unix(), 10)
	endTimeStr := strconv.FormatInt(currentTime.Unix(), 10)

	params.Add("start", startTimeStr)
	params.Add("end", endTimeStr)
	params.Add("step", "30s")

	return fmt.Sprintf("%s?%s", baseURL, params.Encode())
}

func get(req string, resp []byte) (map[string]decimal.Decimal, error) {
	fmt.Println(string(resp))
	return nil, nil
	// var promResp *inspect.PromResp
	// if err := json.Unmarshal(resp, &promResp); err != nil {
	// 	return nil, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	// }

	// if promResp.Status != "success" {
	// 	return nil, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	// }

	// instAvg := make(map[string]decimal.Decimal)
	// for _, res := range promResp.Data.Result {
	// 	totalVal := decimal.NewFromInt(0)

	// 	metric := res.Metric.(map[string]interface{})

	// 	valNums := len(res.Values)
	// 	for _, r := range res.Values {
	// 		val, err := decimal.NewFromString(r[1].(string))
	// 		if err != nil {
	// 			return nil, fmt.Errorf("parse uint value [%s] failed: %v", r[1], err)
	// 		}
	// 		totalVal = totalVal.Add(val)
	// 	}
	// 	fmt.Println(totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2))

	// 	instAvg[metric["instance"].(string)] = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
	// }
	// return instAvg, nil
}
