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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/utils/cluster/ctxt"
	"github.com/wentaojin/tidba/utils/cluster/executor"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"github.com/wentaojin/tidba/utils/cluster/printer"
	"github.com/wentaojin/tidba/utils/cluster/task"
	"github.com/wentaojin/tidba/utils/request"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

const ClusterInspectMinDatabaseVersionRequire = "6.5.0"

type Insepctor struct {
	ctx              context.Context
	startTime        time.Time
	endTime          time.Time
	connector        database.Database
	inspConfig       *InspectConfig
	label            *operator.ClusterLabel
	topo             *operator.ClusterTopology
	logger           *printer.Logger
	nodeExporterPort string
	deployUserSshDir string
	ssh, proxy       *operator.SSHConnectionProps
	gOpt             *operator.Options
}

func NewInspector(ctx context.Context, clusterPath, clusterName, sshDir string, inspCfg *InspectConfig, l *printer.Logger, s, p *operator.SSHConnectionProps, gOpt *operator.Options) (*Insepctor, error) {
	l.Infof("+ Display cluster topology")

	topo, err := operator.GetDeployedClusterTopology(clusterName)
	if err != nil {
		return nil, err
	}

	l.Infof("+ Display cluster labels")

	lb, err := operator.GetDeployedClusterLabels(clusterName)
	if err != nil {
		return nil, err
	}

	port, err := operator.GetDeployedClusterNodeExporterPorrt(clusterPath)
	if err != nil {
		return nil, err
	}

	connector, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster [%s] database connector: %v", clusterName, err)
	}

	return &Insepctor{
		ctx:              ctx,
		connector:        connector,
		inspConfig:       inspCfg,
		label:            lb,
		topo:             topo,
		logger:           l,
		gOpt:             gOpt,
		nodeExporterPort: port,
		ssh:              s,
		proxy:            p,
		deployUserSshDir: sshDir,
	}, nil
}

func (i *Insepctor) GenInspectionWindow() {
	i.endTime = time.Now().UTC()
	i.startTime = i.endTime.Add(-time.Duration(i.inspConfig.WindowMinutes/60) * time.Hour)
}

func (i *Insepctor) GenPDServerAPIPrefix() (string, error) {
	insts, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNamePD)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%d/pd/api/v1", insts[0].Host, insts[0].Port), nil
}

func (i *Insepctor) GenPrometheusAPIPrefix(qpsQuery string, startTs, endTs time.Time) (string, error) {
	insts, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNamePrometheus)
	if err != nil {
		return "", err
	}
	baseURL := fmt.Sprintf("http://%s:%d/api/v1/query_range", insts[0].Host, insts[0].Port)

	// Prepare query parameters
	params := url.Values{}
	params.Add("query", qpsQuery)

	startTimeStr := strconv.FormatInt(startTs.Unix(), 10)
	endTimeStr := strconv.FormatInt(endTs.Unix(), 10)

	params.Add("start", startTimeStr)
	params.Add("end", endTimeStr)
	params.Add("step", "30s")

	return fmt.Sprintf("%s?%s", baseURL, params.Encode()), nil
}

func (i *Insepctor) GenNgMonitorAPIPrefix(startSecs, endSecs int64, comp, instAddr string, top int) (string, error) {
	promp, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNamePrometheus)
	if err != nil {
		return "", err
	}
	portSli := strings.Split(promp[0].Ports, "/")
	if len(portSli) < 2 {
		return "", fmt.Errorf("prometheus ng monitor port not found, ports: [%v]", promp[0].Ports)
	}
	return fmt.Sprintf("http://%s:%s/topsql/v1/summary?end=%d&instance=%s&instance_type=%s&start=%d&top=%d", promp[0].Host, portSli[1], endSecs, instAddr, comp, startSecs, top), nil
}

type PromResp struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}

type Data struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

type Result struct {
	Metric interface{}     `json:"metric"`
	Values [][]interface{} `json:"values"`
}

func (i *Insepctor) GetPromRequestAvgValueByNonMetric(req string, resp []byte) (decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return decimal.Decimal{}, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return decimal.Decimal{}, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	totalRes := decimal.NewFromInt(0)

	resNums := len(promResp.Data.Result)
	for _, res := range promResp.Data.Result {
		totalVal := decimal.NewFromInt(0)

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
				return decimal.Decimal{}, fmt.Errorf("parse uint value [%s] failed: %v", r[1], err)
			}
			totalVal = totalVal.Add(val)
		}
		totalRes = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
	}
	return totalRes.DivRound(decimal.NewFromInt(int64(resNums)), 2), nil
}

func (i *Insepctor) GetPromRequestMaxValueByNonMetric(req string, resp []byte) (decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return decimal.Decimal{}, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return decimal.Decimal{}, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	maxRes := decimal.NewFromInt(0)
	for _, res := range promResp.Data.Result {
		maxVal := decimal.NewFromInt(0)
		for _, r := range res.Values {
			var valStr string
			if r[1] == nil || r[1].(string) == "NaN" {
				valStr = "0"
			} else {
				valStr = r[1].(string)
			}
			val, err := decimal.NewFromString(valStr)
			if err != nil {
				return decimal.Decimal{}, fmt.Errorf("parse uint value [%s] failed: %v", r[1], err)
			}
			if maxVal.LessThan(val) {
				maxVal = val
			}
		}
		if maxRes.LessThan(maxVal) {
			maxRes = maxVal
		}
	}
	return maxRes, nil
}

func (i *Insepctor) GetPromRequestCurrentValueByNonMetric(req string, resp []byte) (decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return decimal.Decimal{}, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return decimal.Decimal{}, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	totalRes := decimal.NewFromInt(0)
	resNums := len(promResp.Data.Result)
	for _, res := range promResp.Data.Result {
		valNums := len(res.Values) - 1
		var valStr string
		if res.Values[valNums][1] == nil || res.Values[valNums][1].(string) == "NaN" {
			valStr = "0"
		} else {
			valStr = res.Values[valNums][1].(string)
		}
		val, err := decimal.NewFromString(valStr)
		if err != nil {
			return decimal.Decimal{}, fmt.Errorf("parse uint value [%s] failed: %v", res.Values[valNums][1], err)
		}
		totalRes = totalRes.Add(val)
	}

	return totalRes.DivRound(decimal.NewFromInt(int64(resNums)), 2), nil
}

func (i *Insepctor) GetPromRequestAvgValueByMetric(req string, resp []byte, pdRegionReq ...bool) (map[string]decimal.Decimal, error) {
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

func (i *Insepctor) GetPromRequestAvgDiskValueByMetric(req string, resp []byte) (map[string]map[string]decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return nil, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	instAvg := make(map[string]map[string]decimal.Decimal)
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

		if val, exist := instAvg[metric["instance"].(string)]; exist {
			val[metric["device"].(string)] = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
			instAvg[metric["instance"].(string)] = val
		} else {
			deviceAvg := make(map[string]decimal.Decimal)
			deviceAvg[metric["device"].(string)] = totalVal.DivRound(decimal.NewFromInt(int64(valNums)), 2)
			instAvg[metric["instance"].(string)] = deviceAvg
		}
	}
	return instAvg, nil
}

func (i *Insepctor) GetPromRequestMaxValueByMetric(req string, resp []byte, pdRegionReq ...bool) (map[string]decimal.Decimal, error) {
	var promResp *PromResp
	if err := json.Unmarshal(resp, &promResp); err != nil {
		return nil, fmt.Errorf("unmarsh prometheus response failed: %v", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query request [%s] failed, status: [%v]", req, promResp.Status)
	}

	instMax := make(map[string]decimal.Decimal)

	for _, res := range promResp.Data.Result {
		maxVal := decimal.NewFromInt(0)

		metric := res.Metric.(map[string]interface{})

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
			if maxVal.LessThan(val) {
				maxVal = val
			}
		}
		if len(pdRegionReq) > 0 {
			instMax[metric["address"].(string)] = maxVal
		} else {
			instMax[metric["instance"].(string)] = maxVal
		}
	}
	return instMax, nil
}

func (i *Insepctor) InspClusterDatabaseVersion() error {
	i.logger.Infof("+ Inspect cluster version")
	version := i.topo.ClusterMeta.ClusterVersion
	if stringutil.VersionOrdinal(strings.TrimPrefix(version, "v")) >= stringutil.VersionOrdinal(ClusterInspectMinDatabaseVersionRequire) {
		return nil
	}
	return fmt.Errorf("the cluster [%s] version is %v, which is lower than %v. inspect exits", i.topo.ClusterMeta.ClusterName, version, ClusterInspectMinDatabaseVersionRequire)
}

func (i *Insepctor) InspClusterTopSqlIsEnable() error {
	// the topsql function must be activated for tidb/tikv cpu dimension sql
	if i.inspConfig.Modules.CheckSQLOrderByTidbCPUTime || i.inspConfig.Modules.CheckSQLOrderByTikvCPUTime {
		database := i.connector.(*mysql.Database)
		_, res, err := database.GeneralQuery(i.ctx, `SELECT CURRENT_VALUE FROM INFORMATION_SCHEMA.VARIABLES_INFO where variable_name <>'tidb_config'`)
		if err != nil {
			return err
		}
		if res[0]["CURRENT_VALUE"] == "ON" {
			return nil
		}
		return fmt.Errorf("to enable tidb/tikv cpu dimension sql inspection, you must activate the topsql function [SET GLOBAL tidb_enable_top_sql = 1;]. Please enable topsql collection and then inspect, or disable the tidb/tikv cpu dimension sql inspection function to bypass it")
	}
	return nil
}

func (i *Insepctor) InspBasicHardwares() ([]*BasicHardware, error) {
	var hardwaresTasks []*task.StepDisplay

	for _, ip := range i.topo.GetClusterTopologyHostIps() {
		tf := task.NewBuilder(i.logger).
			RootSSH(
				ip,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(ip, `lscpu | grep "Architecture" | awk -F ":" "{print \$2}" | xargs || echo "N/A"`, fmt.Sprintf("%s@cpu_arch", ip), true).
			Shell(ip, `lscpu | grep "^CPU(s):" | awk -F ":" "{print \$2}" | xargs || echo "N/A"`, fmt.Sprintf("%s@cpu_vcore", ip), true).
			Shell(ip, `(command -v numactl > /dev/null && numactl --hardware | grep "available:" | xargs) || echo "N/A"`, fmt.Sprintf("%s@numa", ip), true).
			Shell(ip, `free -h | awk "/Mem:/ {print \$2}" || echo "N/A"`, fmt.Sprintf("%s@memory", ip), true).
			Shell(ip, `cat /etc/os-release | grep "PRETTY_NAME" | cut -d "\"" -f2 || echo "N/A"`, fmt.Sprintf("%s@version", ip), true).
			BuildAsStep(fmt.Sprintf("  - Inspect machine %s hardware", ip))
		hardwaresTasks = append(hardwaresTasks, tf)
	}
	if len(hardwaresTasks) == 0 {
		return nil, nil
	}

	ctx := ctxt.New(
		i.ctx,
		i.gOpt.Concurrency,
		i.logger,
	)
	t := task.NewBuilder(i.logger).
		ParallelStep("+ Inspect machine hardware", false, hardwaresTasks...).Build()

	if err := t.Execute(ctx); err != nil {
		return nil, fmt.Errorf("failed to fetch machine hardware, error detail: %v", err)
	}
	var bhs []*BasicHardware

	for _, ip := range i.topo.GetClusterTopologyHostIps() {
		cpuArch := fmt.Sprintf("%s@cpu_arch", ip)
		stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(cpuArch)
		if !ok {
			return nil, fmt.Errorf("no check results found for %s", cpuArch)
		}
		arch := strings.Trim(string(stdout), "\n")

		cpuVcore := fmt.Sprintf("%s@cpu_vcore", ip)
		stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(cpuVcore)
		if !ok {
			return nil, fmt.Errorf("no check results found for %s", cpuVcore)
		}
		vcore := strings.Trim(string(stdout), "\n")

		numa := fmt.Sprintf("%s@numa", ip)
		stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(numa)
		if !ok {
			return nil, fmt.Errorf("no check results found for %s", numa)
		}
		numa = strings.Trim(string(stdout), "\n")

		memory := fmt.Sprintf("%s@memory", ip)
		stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(memory)
		if !ok {
			return nil, fmt.Errorf("no check results found for %s", memory)
		}
		memory = strings.Trim(string(stdout), "\n")

		version := fmt.Sprintf("%s@version", ip)
		stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(version)
		if !ok {
			return nil, fmt.Errorf("no check results found for %s", version)
		}
		version = strings.Trim(string(stdout), "\n")

		bhs = append(bhs, &BasicHardware{
			IpAddress: ip,
			CpuArch:   arch,
			CpuVcore:  vcore,
			Numa:      numa,
			Memory:    memory,
			OsVersion: version,
		})
	}
	return bhs, nil
}

func (i *Insepctor) InspClusterSoftware() ([]*BasicSoftware, error) {
	i.logger.Infof("+ Inspect cluster software")

	var bs []*BasicSoftware

	db := i.connector.(*mysql.Database)
	_, res, err := db.GeneralQuery(i.ctx, `SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}
	bs = append(bs, &BasicSoftware{
		Category: "数据库版本",
		Value:    res[0]["VERSION"],
	})

	pdAPI, err := i.GenPDServerAPIPrefix()
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			resp, err := request.Request(request.DefaultRequestMethodGet, fmt.Sprintf("%s/cluster", pdAPI), nil, "", "")
			if err != nil {
				return err
			}

			esp := make(map[string]uint64)
			if err := json.Unmarshal(resp, &esp); err != nil {
				return err
			}
			bs = append(bs, &BasicSoftware{
				Category: "数据库集群 ID",
				Value:    strconv.FormatUint(esp["id"], 10),
			})
			return nil
		},
	); err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			prompReq, err := i.GenPrometheusAPIPrefix(`sum(pd_cluster_status{type="leader_count"})`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}

			regions, err := i.GetPromRequestAvgValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}
			bs = append(bs, &BasicSoftware{
				Category: "Region 数量（多副本）",
				Value:    regions.String(),
			})
			return nil
		},
	); err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			prompReq, err := i.GenPrometheusAPIPrefix(`sum(pd_cluster_status{type="storage_size"})`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}

			sizes, err := i.GetPromRequestAvgValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}

			newSizeGB := sizes.DivRound(decimal.NewFromInt(1024*1024*1024), 2)
			bs = append(bs, &BasicSoftware{
				Category: "已用数据库空间（多副本）",
				Value:    fmt.Sprintf("%vGB", newSizeGB.String()),
			})
			return nil
		},
	); err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			prompReq, err := i.GenPrometheusAPIPrefix(`sum(rate(tidb_executor_statement_total[30s]))`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}

			value, err := i.GetPromRequestMaxValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}

			bs = append(bs, &BasicSoftware{
				Category: fmt.Sprintf("QPS 峰值（%.2fH）", float64(i.inspConfig.WindowMinutes/60)),
				Value:    value.Round(2).String(),
			})
			return nil
		},
	); err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			prompReq, err := i.GenPrometheusAPIPrefix(`histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket[30s])) by (le))`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}

			value, err := i.GetPromRequestAvgValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}

			bs = append(bs, &BasicSoftware{
				Category: fmt.Sprintf("SQL duration P99 均值（%.2fH）", float64(i.inspConfig.WindowMinutes/60)),
				Value:    value.String(),
			})
			return nil
		},
	); err != nil {
		return nil, err
	}

	return bs, nil
}

func (i *Insepctor) InspClusterTopology() []*ClusterTopology {
	i.logger.Infof("+ Inspect cluster topology")

	var cls []*ClusterTopology

	for host, comps := range i.topo.GetClusterMachineComponentInstances() {
		var coms []string
		for k, v := range comps {
			coms = append(coms, fmt.Sprintf("%s * %d", k, v))
		}
		cls = append(cls, &ClusterTopology{
			IpAddress:  host,
			Components: strings.Join(coms, ", "),
		})
	}
	return cls
}

func (i *Insepctor) InspClusterSummary() ([]*ClusterSummary, error) {
	i.logger.Infof("+ Inspect cluster summary")

	var cs []*ClusterSummary
	panicInsts := i.topo.GetClusterComponentInstanceNonUpStatus()

	var resDesc []string

	if len(panicInsts) == 0 {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "实例状态检查",
			CheckBaseline: "是否所有组件为 UP 状态",
			CheckResult:   "正常",
			ResultDesc:    "所有实例均为 UP 状态",
		})
	} else {
		for inst, comps := range panicInsts {
			for k, v := range comps {
				resDesc = append(resDesc, fmt.Sprintf("异常实例：%s\n角色：%s\n状态：%s\n---", inst, k, v))
			}
		}

		cs = append(cs, &ClusterSummary{
			CheckItem:     "实例状态检查",
			CheckBaseline: "是否所有组件为 UP 状态",
			CheckResult:   "异常",
			ResultDesc:    strings.Join(resDesc, "\n"),
		})
		resDesc = []string{}
	}

	db := i.connector.(*mysql.Database)
	_, res, err := db.GeneralQuery(i.ctx, `SELECT TYPE, INSTANCE, START_TIME FROM INFORMATION_SCHEMA.CLUSTER_INFO`)
	if err != nil {
		return nil, err
	}

	currentTime := time.Now().UTC()

	for _, r := range res {
		// Parse start time with timezone
		startTimestamp, err := time.Parse(time.RFC3339, r["START_TIME"])
		if err != nil {
			return nil, fmt.Errorf("error parsing start time: %v", err)
		}

		// Calculate time difference in days
		timeDiff := int(currentTime.Sub(startTimestamp).Hours() / 24)

		// Check if time difference is less than 30 days
		if timeDiff < 30 {
			resDesc = append(resDesc, fmt.Sprintf("异常实例：%s\n角色：%s\n启动时间：%s\n---", r["INSTANCE"], r["TYPE"], r["START_TIME"]))
		}
	}

	if len(resDesc) == 0 {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "实例启动时间检查",
			CheckBaseline: "是否最近一个月内未发生过重启",
			CheckResult:   "正常",
			ResultDesc:    "无近期重启实例",
		})
	} else {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "实例启动时间检查",
			CheckBaseline: "是否最近一个月内未发生过重启",
			CheckResult:   "异常",
			ResultDesc:    strings.Join(resDesc, "\n"),
		})
		resDesc = []string{}
	}

	_, res, err = db.GeneralQuery(i.ctx, `SELECT 
    TYPE, 
    GROUP_CONCAT(DISTINCT GIT_HASH) AS GIT_HASHES, 
    COUNT(DISTINCT GIT_HASH) AS HASH_COUNT
FROM 
    INFORMATION_SCHEMA.CLUSTER_INFO
GROUP BY 
    TYPE
HAVING 
    COUNT(DISTINCT GIT_HASH) > 1`)
	if err != nil {
		return nil, err
	}
	for _, r := range res {
		hashCount, err := strconv.ParseInt(r["HASH_COUNT"], 10, 64)
		if err != nil {
			return nil, err
		}
		if hashCount > 1 {
			resDesc = append(resDesc, fmt.Sprintf("组件类型：%s, GIT_HASH 列表：%s\n---", r["TYPE"], r["GIT_HASHES"]))
		}
	}
	if len(resDesc) == 0 {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "组件版本检查",
			CheckBaseline: "是否存在多个组件版本",
			CheckResult:   "正常",
			ResultDesc:    "无异常组件",
		})
	} else {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "组件版本检查",
			CheckBaseline: "是否存在多个组件版本",
			CheckResult:   "异常",
			ResultDesc:    strings.Join(resDesc, "\n"),
		})
		resDesc = []string{}
	}

	machineMap := make(map[string]map[string]*operator.Label)

	for _, label := range i.label.Labels {
		if machineMap[label.Machine] == nil {
			machineMap[label.Machine] = make(map[string]*operator.Label)
		}
		machineMap[label.Machine][label.Labels] = label
	}

	for machine, labelMap := range machineMap {
		if len(labelMap) > 1 {
			resDesc = append(resDesc, fmt.Sprintf("异常：machine [%s] 同台机器不同 labels", machine))
			for _, label := range labelMap {
				resDesc = append(resDesc, fmt.Sprintf("- instance: %s:%s, labels: %s", label.Machine, label.Port, label.Labels))
			}
		}
	}

	labelMap := make(map[string]map[string]*operator.Label)

	for _, label := range i.label.Labels {
		if labelMap[label.Labels] == nil {
			labelMap[label.Labels] = make(map[string]*operator.Label)
		}
		labelMap[label.Labels][label.Machine] = label
	}

	for labels, machineMap := range labelMap {
		if len(machineMap) > 1 {
			resDesc = append(resDesc, fmt.Sprintf("异常：label [%s] 同个标签不同机器", labels))
			for _, label := range machineMap {
				resDesc = append(resDesc, fmt.Sprintf("- instance: %s:%s, labels: %s", label.Machine, label.Port, label.Labels))
			}
		}
	}

	if len(resDesc) == 0 {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "label 检查",
			CheckBaseline: "是否符合行内标准",
			CheckResult:   "正常",
			ResultDesc:    "无异常",
		})
	} else {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "label 检查",
			CheckBaseline: "是否符合行内标准",
			CheckResult:   "异常",
			ResultDesc:    strings.Join(resDesc, "\n"),
		})
		resDesc = []string{}
	}

	re := regexp.MustCompile(`(\d+\.\d+|\d+)`)

	for _, label := range i.label.Labels {
		avaMatch := re.FindStringSubmatch(label.Available)

		var (
			available float64
			capacity  float64
			err       error
		)
		if len(avaMatch) > 1 {
			available, err = strconv.ParseFloat(avaMatch[1], 64)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("find string submatch available [%s] not found number", label.Available)
		}

		capMatch := re.FindStringSubmatch(label.Capacity)
		if len(capMatch) > 1 {
			capacity, err = strconv.ParseFloat(capMatch[1], 64)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("find string submatch capacity [%s] not found number", label.Capacity)
		}

		usedPercent := float64((capacity - available) / capacity)
		if usedPercent > 0.7 {
			resDesc = append(resDesc, fmt.Sprintf("instance [%s:%s] 对应磁盘使用率过高（使用率: %2.f）", label.Machine, label.Port, usedPercent))
		}
	}

	if len(resDesc) == 0 {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "容量检查",
			CheckBaseline: "容量是否超过 70%",
			CheckResult:   "正常",
			ResultDesc:    "所有节点磁盘容量正常",
		})
	} else {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "容量检查",
			CheckBaseline: "容量是否超过 70%",
			CheckResult:   "异常",
			ResultDesc:    strings.Join(resDesc, "\n"),
		})
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			prompReq, err := i.GenPrometheusAPIPrefix(`pd_regions_status{type="empty-region-count"}`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}

			emptyRegions, err := i.GetPromRequestCurrentValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}

			prompReq, err = i.GenPrometheusAPIPrefix(`sum(pd_cluster_status{type="leader_count"})`, i.startTime, i.endTime)
			if err != nil {
				return err
			}
			resp, err = request.Request(request.DefaultRequestMethodGet, prompReq, nil, "", "")
			if err != nil {
				return err
			}
			leaderCounts, err := i.GetPromRequestCurrentValueByNonMetric(prompReq, resp)
			if err != nil {
				return err
			}

			decimalZer0 := decimal.NewFromInt(0)
			decimal100000 := decimal.NewFromInt(100000)

			if emptyRegions.Equal(decimalZer0) && leaderCounts.Equal(decimalZer0) {
				cs = append(cs, &ClusterSummary{
					CheckItem:     "空 region 情况",
					CheckBaseline: "空 region 数占比 30% 且空 region 是否超过 10W",
					CheckResult:   "异常",
					ResultDesc:    "无法获取 empty-region-count 或 leader_count 的值",
				})
			} else if emptyRegions.GreaterThanOrEqual(decimal100000) && emptyRegions.DivRound(leaderCounts, 2).GreaterThanOrEqual(decimal.NewFromFloat(0.3)) {
				cs = append(cs, &ClusterSummary{
					CheckItem:     "空 region 情况",
					CheckBaseline: "空 region 数占比 30% 且空 region 是否超过 10W",
					CheckResult:   "异常",
					ResultDesc:    fmt.Sprintf("空 region 数：%v, 占比：%v%%", emptyRegions.String(), emptyRegions.DivRound(leaderCounts, 2).Mul(decimal.NewFromInt(100))),
				})
			} else {
				cs = append(cs, &ClusterSummary{
					CheckItem:     "空 region 情况",
					CheckBaseline: "空 region 数占比 30% 且空 region 是否超过 10W",
					CheckResult:   "正常",
					ResultDesc:    fmt.Sprintf("空 region 数：%v, 占比：%v%%", emptyRegions.String(), emptyRegions.DivRound(leaderCounts, 2).Mul(decimal.NewFromInt(100))),
				})
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			var standardSchedulers = []string{
				"balance-leader-scheduler",
				"balance-hot-region-scheduler",
				"split-bucket-scheduler",
				"balance-region-scheduler"}

			// abnormal scheduler list (for emergency use, should not exist for a long time)
			var emergencySchedulers = []string{
				"evict-leader-scheduler",
				"scatter-range-scheduler",
			}

			pdAPI, err := i.GenPDServerAPIPrefix()
			if err != nil {
				return err
			}
			resp, err := request.Request(request.DefaultRequestMethodGet, fmt.Sprintf("%s/schedulers", pdAPI), nil, "", "")
			if err != nil {
				return err
			}

			var currentSchedulers []string

			err = json.Unmarshal(resp, &currentSchedulers)
			if err != nil {
				return fmt.Errorf("error unmarshalling JSON: %v", err)
			}

			stadnardSet := stringutil.NewStringSet(standardSchedulers...)
			missStandards := stadnardSet.Difference(stringutil.NewStringSet(currentSchedulers...))

			unexpectedSches := stringutil.NewStringSet(emergencySchedulers...).Intersection(stringutil.NewStringSet(currentSchedulers...))

			if len(missStandards.Slice()) == 0 && len(unexpectedSches.Slice()) == 0 {
				cs = append(cs, &ClusterSummary{
					CheckItem:     "检查存在的调度器",
					CheckBaseline: "所有必要的调度器存在；无异常调度器",
					CheckResult:   "正常",
					ResultDesc:    fmt.Sprintf("当前调度器：\n%s", strings.Join(currentSchedulers, "\n")),
				})
			} else {
				var b strings.Builder
				if len(missStandards.Slice()) > 0 {
					b.WriteString(fmt.Sprintf("\n缺少以下标准调度器：\n%s", strings.Join(missStandards.Slice(), "\n")))
				}
				if len(unexpectedSches.Slice()) > 0 {
					b.WriteString(fmt.Sprintf("\n发现以下异常调度器：\n%s", strings.Join(unexpectedSches.Slice(), "\n")))
				}
				cs = append(cs, &ClusterSummary{
					CheckItem:     "检查存在的调度器",
					CheckBaseline: "所有必要的调度器存在；无异常调度器",
					CheckResult:   "异常",
					ResultDesc:    fmt.Sprintf("当前调度器：\n%s%s", strings.Join(currentSchedulers, "\n"), b.String()),
				})
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	_, res, err = db.GeneralQuery(i.ctx, `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME IN ('tikv_gc_last_run_time', 'tikv_gc_safe_point', 'tikv_gc_life_time')`)
	if err != nil {
		return nil, err
	}

	customTimeFormat := "20060102-15:04:05.000 +0800"
	var (
		currentTimeUnix       int64
		gcLastRunUnix         int64
		gcSafePointUnix       int64
		gcLifeTimeHour        float64
		originGcLastRunTime   string
		originGcSafePointTime string
		originGcLifeTime      string
	)
	currentTimeNow := time.Now()
	currentTimeUnix = currentTimeNow.Unix()
	currentTimeStr := currentTimeNow.UTC().Format("2006-01-02 15:04:05.000")

	for _, r := range res {
		if strings.EqualFold(r["VARIABLE_NAME"], "tikv_gc_last_run_time") {
			customTime, err := time.Parse(customTimeFormat, r["VARIABLE_VALUE"])
			if err != nil {
				return nil, fmt.Errorf("parse tikv_gc_last_run_time time [%s] failed: %v", r["VARIABLE_VALUE"], err)
			}
			gcLastRunUnix = customTime.Unix()
			originGcLastRunTime = customTime.UTC().Format("2006-01-02 15:04:05.000")
		} else if strings.EqualFold(r["VARIABLE_NAME"], "tikv_gc_safe_point") {
			customTime, err := time.Parse(customTimeFormat, r["VARIABLE_VALUE"])
			if err != nil {
				return nil, fmt.Errorf("parse tikv_gc_safe_point time [%s] failed: %v", r["VARIABLE_VALUE"], err)
			}
			gcSafePointUnix = customTime.Unix()
			originGcSafePointTime = customTime.UTC().Format("2006-01-02 15:04:05.000")
		} else {
			gcLifeTimeHour, err = stringutil.ParseDurationConvertHours(r["VARIABLE_VALUE"])
			if err != nil {
				return nil, fmt.Errorf("parse tikv_gc_life_time duration convert hour [%s] failed: %v", r["VARIABLE_VALUE"], err)
			}
			originGcLifeTime = r["VARIABLE_VALUE"]
		}
	}

	diffLastRunToNowHourInterval := stringutil.TimeUnixToHours(currentTimeUnix - gcLastRunUnix)
	diffLastRunToSafeHourInterval := stringutil.TimeUnixToHours(gcLastRunUnix - gcSafePointUnix)

	if diffLastRunToNowHourInterval <= 24 && diffLastRunToSafeHourInterval <= gcLifeTimeHour {
		cs = append(cs, &ClusterSummary{
			CheckItem:     "GC 是否正常",
			CheckBaseline: "tikv_gc_last_run_time 和 tikv_gc_safe_point 相差不超过 tikv_gc_life_time；\ntikv_gc_last_run_time 和当前时间相差不超过 1 天",
			CheckResult:   "正常",
			ResultDesc:    fmt.Sprintf("gc 工作正常：\ntikv_gc_last_run_time: %s\ntikv_gc_safe_point: %s\ntidb_gc_life_time 变量参数： %s", originGcLastRunTime, originGcSafePointTime, originGcLifeTime),
		})
	} else {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("系统参数：\ntikv_gc_last_run_time: %s\ntikv_gc_safe_point: %s\n变量参数 tidb_gc_life_time: %s", originGcLastRunTime, originGcSafePointTime, originGcLifeTime))
		b.WriteString("异常信息：\n")
		if diffLastRunToNowHourInterval > 24 {
			b.WriteString(fmt.Sprintf("GC 被阻塞，tikv_gc_last_run_time 上次运行时间距离当前时间 [%s] 超过 1 天；\n", currentTimeStr))
		}
		if diffLastRunToSafeHourInterval > gcLifeTimeHour {
			b.WriteString(fmt.Sprintf("GC 速度运行缓慢，变量参数 tidb_gc_life_time [%s] 可能设置过大；", originGcLifeTime))
		}
		cs = append(cs, &ClusterSummary{
			CheckItem:     "GC 是否正常",
			CheckBaseline: "tikv_gc_last_run_time 和 tikv_gc_safe_point 相差不超过 tikv_gc_life_time；\ntikv_gc_last_run_time 和当前时间相差不超过 1 天",
			CheckResult:   "异常",
			ResultDesc:    b.String(),
		})
	}

	return cs, nil
}

func (i *Insepctor) InspDevBestPractices() ([]*DevBestPractice, bool, []*InspDevBestPracticesAbnormalOutput, error) {
	i.logger.Infof("+ Inspect development best practices")

	var (
		devBests           []*DevBestPractice
		devAbnormalOutputs []*InspDevBestPracticesAbnormalOutput
	)

	database := i.connector.(*mysql.Database)

	_, res, err := database.GeneralQuery(i.ctx, `select version() AS VERSION`)
	if err != nil {
		return nil, false, nil, err
	}
	/*
		select version();
		+--------------------+
		| version()          |
		+--------------------+
		| 5.7.25-TiDB-v6.5.6 |
		+--------------------+
		1 row in set (0.00 sec)
	*/
	versionSli := strings.Split(res[0]["VERSION"], "-")
	version := strings.Trim(versionSli[2], "v")

	globalExceedFlag := false
	for seq, dbp := range DefaultDevBestPracticesInspItems() {
		if seq == 26 {
			// JSON >= v6.5.0
			if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal("6.5.0") {
				devBests = append(devBests, &DevBestPractice{
					CheckItem:         dbp.CheckItem,
					CheckCategory:     dbp.CheckCategory,
					CorrectionSuggest: dbp.RectificationType,
					BestPracticeDesc:  dbp.BestPracticeDesc,
					CheckResult:       "正常",
					AbnormalDetail:    fmt.Sprintf("数据库版本 [%v] 符合 JSON 数据类型启用最低要求", version),
				})
				continue
			}
		} else if seq == 27 {
			// Partition >= v6.5.0
			if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal("6.5.0") {
				devBests = append(devBests, &DevBestPractice{
					CheckItem:         dbp.CheckItem,
					CheckCategory:     dbp.CheckCategory,
					CorrectionSuggest: dbp.RectificationType,
					BestPracticeDesc:  dbp.BestPracticeDesc,
					CheckResult:       "正常",
					AbnormalDetail:    fmt.Sprintf("数据库版本 [%v] 符合分区表功能特性启用最低要求", version),
				})
				continue
			}
		}

		_, res, err := database.GeneralQuery(i.ctx, dbp.CheckSql)
		if err != nil {
			return nil, false, nil, err
		}
		var results []string
		for _, r := range res {
			results = append(results, r["SQL_RESULT"])
		}

		if len(results) == 0 {
			devBests = append(devBests, &DevBestPractice{
				CheckItem:         dbp.CheckItem,
				CheckCategory:     dbp.CheckCategory,
				CorrectionSuggest: dbp.RectificationType,
				BestPracticeDesc:  dbp.BestPracticeDesc,
				CheckResult:       "正常",
				AbnormalDetail:    "无",
			})
		} else {
			// Note: In exceptional cases, only the first 50 characters of results are displayed.
			var (
				chunkS      []string
				isExceed    bool
				abnormalStr string
			)

			abnormalCounts := len(results)
			if abnormalCounts > 50 {
				results = results[:51]
				isExceed = true
				globalExceedFlag = true
			}

			chunkSli := stringutil.ChunkStrings(results, 5)
			for _, c := range chunkSli {
				chunkS = append(chunkS, strings.Join(c, ", "))
			}

			if isExceed {
				abnormalStr = fmt.Sprintf("警告：超过 50 张表不符合要求(仅列 50 张)\n%s\n...", strings.Join(chunkS, "\n"))
			} else {
				abnormalStr = strings.Join(chunkS, "\n")
			}
			devBests = append(devBests, &DevBestPractice{
				CheckItem:         dbp.CheckItem,
				CheckCategory:     dbp.CheckCategory,
				CorrectionSuggest: dbp.RectificationType,
				BestPracticeDesc:  dbp.BestPracticeDesc,
				CheckResult:       "异常",
				AbnormalDetail:    abnormalStr,
			})

			devAbnormalOutputs = append(devAbnormalOutputs, &InspDevBestPracticesAbnormalOutput{
				InspDevBestPractices: dbp,
				AbnormalDetail:       strings.Join(results, "\n"),
				AbnormalCounts:       abnormalCounts,
			})
		}
	}
	return devBests, globalExceedFlag, devAbnormalOutputs, nil
}

func (i *Insepctor) InspDatabaseVaribale() ([]*DatabaseVaribale, error) {
	i.logger.Infof("+ Inspect database variable practices")

	var dv []*DatabaseVaribale

	database := i.connector.(*mysql.Database)

	_, res, err := database.GeneralQuery(i.ctx, `SELECT VARIABLE_NAME, CURRENT_VALUE, DEFAULT_VALUE FROM INFORMATION_SCHEMA.VARIABLES_INFO where variable_name <>'tidb_config'`)
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		val, exist := i.inspConfig.VariablesParams[r["VARIABLE_NAME"]]
		if exist {
			valStr, err := stringutil.FormatInterfaceToString(val)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(r["CURRENT_VALUE"], valStr) {
				dv = append(dv, &DatabaseVaribale{
					Component:     operator.ComponentNameTiDB,
					ParamName:     r["VARIABLE_NAME"],
					DefaultValue:  r["DEFAULT_VALUE"],
					CurrentValue:  r["CURRENT_VALUE"],
					StandardValue: valStr,
					IsStandard:    "否",
				})
			}
		}
	}
	return dv, nil
}

func (i *Insepctor) InspDatabaseConfig() ([]*DatabaseConfig, error) {
	i.logger.Infof("+ Inspect database config practices")

	var dv []*DatabaseConfig

	database := i.connector.(*mysql.Database)

	i.logger.Infof("  - Inspect tikv component config practices")
	_, res, err := database.GeneralQuery(i.ctx, "SELECT INSTANCE,`KEY`,`VALUE` FROM INFORMATION_SCHEMA.CLUSTER_CONFIG WHERE `TYPE` = 'tikv'")
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		val, exist := i.inspConfig.TiKVConfigParams[r["KEY"]]
		if exist {
			valStr, err := stringutil.FormatInterfaceToString(val)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(r["VALUE"], valStr) {
				dv = append(dv, &DatabaseConfig{
					Component:     operator.ComponentNameTiKV,
					Instance:      r["INSTANCE"],
					ParamName:     r["KEY"],
					CurrentValue:  r["VALUE"],
					StandardValue: valStr,
					IsStandard:    "否",
				})
			}
		}
	}

	i.logger.Infof("  - Inspect tidb component config practices")
	_, res, err = database.GeneralQuery(i.ctx, "SELECT INSTANCE,`KEY`,`VALUE` FROM INFORMATION_SCHEMA.CLUSTER_CONFIG WHERE `TYPE` = 'tidb'")
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		val, exist := i.inspConfig.TiDBConfigParams[r["KEY"]]
		if exist {
			valStr, err := stringutil.FormatInterfaceToString(val)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(r["VALUE"], valStr) {
				dv = append(dv, &DatabaseConfig{
					Component:     operator.ComponentNameTiDB,
					Instance:      r["INSTANCE"],
					ParamName:     r["KEY"],
					CurrentValue:  r["VALUE"],
					StandardValue: valStr,
					IsStandard:    "否",
				})
			}
		}
	}

	i.logger.Infof("  - Inspect pd component config practices")
	_, res, err = database.GeneralQuery(i.ctx, "SELECT INSTANCE,`KEY`,`VALUE` FROM INFORMATION_SCHEMA.CLUSTER_CONFIG WHERE `TYPE` = 'pd'")
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		val, exist := i.inspConfig.PDConfigParams[r["KEY"]]
		if exist {
			valStr, err := stringutil.FormatInterfaceToString(val)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(r["VALUE"], valStr) {
				dv = append(dv, &DatabaseConfig{
					Component:     operator.ComponentNamePD,
					Instance:      r["INSTANCE"],
					ParamName:     r["KEY"],
					CurrentValue:  r["VALUE"],
					StandardValue: valStr,
					IsStandard:    "否",
				})
			}
		}
	}
	return dv, nil
}

func (i *Insepctor) InspDatabaseStatistics() ([]*DatabaseStatistics, bool, []*InspDatabaseStatisticsAbnormalOutput, error) {
	i.logger.Infof("+ Inspect database statistics practices")

	var (
		ds                   []*DatabaseStatistics
		statsAbnormalOutputs []*InspDatabaseStatisticsAbnormalOutput
	)

	database := i.connector.(*mysql.Database)

	_, res, err := database.GeneralQuery(i.ctx, `select version() AS VERSION`)
	if err != nil {
		return nil, false, nil, err
	}
	/*
		select version();
		+--------------------+
		| version()          |
		+--------------------+
		| 5.7.25-TiDB-v6.5.6 |
		+--------------------+
		1 row in set (0.00 sec)
	*/
	versionSli := strings.Split(res[0]["VERSION"], "-")
	version := strings.Trim(versionSli[2], "v")

	globalExceedFlag := false
	for seq, dbp := range DefaultInspDatabaseStatisticsItems() {
		if seq == 10 {
			// stats lock >= v6.5.0
			if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal("8.1.0") {
				ds = append(ds, &DatabaseStatistics{
					CheckItem:      dbp.CheckItem,
					CheckStandard:  dbp.CheckStandard,
					CheckResult:    "正常",
					AbnormalDetail: fmt.Sprintf("数据库版本 [%v] 符合锁定统计信息功能启用最低要求", version),
				})
				continue
			}
		}

		_, res, err := database.GeneralQuery(i.ctx, dbp.CheckSql)
		if err != nil {
			return nil, false, nil, err
		}
		var results []string

		for _, r := range res {
			results = append(results, r["SQL_RESULT"])
		}

		if len(results) == 0 {
			ds = append(ds, &DatabaseStatistics{
				CheckItem:      dbp.CheckItem,
				CheckStandard:  dbp.CheckStandard,
				CheckResult:    "正常",
				AbnormalDetail: "无",
			})
		} else {
			// Note: In exceptional cases, only the first 50 characters of results are displayed.
			var (
				chunkS      []string
				abnormalStr string
			)
			isExceed := false

			abnormalCounts := len(results)
			if abnormalCounts > 500 {
				results = results[:501]
				isExceed = true
				globalExceedFlag = true
			}
			chunkSli := stringutil.ChunkStrings(results, 5)
			for _, c := range chunkSli {
				chunkS = append(chunkS, strings.Join(c, ", "))
			}

			if isExceed {
				abnormalStr = fmt.Sprintf("警告：超过 50 张表不符合要求(仅列 50 张)\n%s\n...", strings.Join(chunkS, "\n"))
			} else {
				abnormalStr = strings.Join(chunkS, "\n")
			}

			ds = append(ds, &DatabaseStatistics{
				CheckItem:      dbp.CheckItem,
				CheckStandard:  dbp.CheckStandard,
				CheckResult:    "异常",
				AbnormalDetail: abnormalStr,
			})

			statsAbnormalOutputs = append(statsAbnormalOutputs, &InspDatabaseStatisticsAbnormalOutput{
				InspDatabaseStatistics: dbp,
				AbnormalDetail:         strings.Join(results, "\n"),
				AbnormalCounts:         abnormalCounts,
				Comment:                "",
			})
		}
	}
	return ds, globalExceedFlag, statsAbnormalOutputs, nil
}

func (i *Insepctor) InspSystemConfig() ([]*SystemConfig, []*SystemConfigOutput, error) {
	i.logger.Infof("+ Inspect system config practices")

	// tikv component dir mount point
	var (
		tikvDirPointTasks []*task.StepDisplay
	)
	tikvDirMountPoints := i.topo.GetClusterTiKVComponentHostDirMountPoints()

	sysConfigOutputs := make(map[string][]string)

	for host, points := range tikvDirMountPoints {
		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			)

		for seq, point := range points {
			tf.Shell(host, fmt.Sprintf(`mountpoint -q '%s' && findmnt -no FSTYPE,OPTIONS '/' || echo "no"`, point), fmt.Sprintf("%s_%d", host, seq), true)
		}

		tikvDirPointTasks = append(tikvDirPointTasks, tf.BuildAsStep(fmt.Sprintf("  - Inspect machine %s tikv mount points", host)))
	}

	// disk read/write latency
	// api output
	decimalMs := decimal.NewFromInt(1000)

	i.logger.Infof("  - Inspect host disk write latency")
	diskWriteApi, errMsg := i.GenPrometheusAPIPrefix(`(rate(node_disk_write_time_seconds_total{}[5m])/ rate(node_disk_writes_completed_total{}[5m]))`, i.startTime, i.endTime)
	if errMsg != nil {
		return nil, nil, errMsg
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			var bs strings.Builder
			avgResp, err := request.Request(request.DefaultRequestMethodGet, diskWriteApi, nil, "", "")
			if err != nil {
				return err
			}

			avgVal, err := i.GetPromRequestAvgDiskValueByMetric("host disk avg wirte latency", avgResp)
			if err != nil {
				return err
			}

			for inst, devices := range avgVal {
				var deviceInfos []string
				for d, val := range devices {
					// seconds -> ms
					if val.Mul(decimalMs).LessThan(decimal.NewFromFloat(DefaultHostDiskWriteLatencyUnderline * 1000)) {
						deviceInfos = append(deviceInfos, fmt.Sprintf("- %s", d))
					}
				}

				if len(deviceInfos) > 0 {
					bs.WriteString("检查以下主机磁盘平均写延迟超过 10ms:\n")
					instSli := strings.Split(inst, ":")
					bs.WriteString(strings.Join(deviceInfos, "\n"))
					sysConfigOutputs[instSli[0]] = append(sysConfigOutputs[instSli[0]], bs.String())
				}
			}
			return nil
		},
	); err != nil {
		return nil, nil, err
	}

	i.logger.Infof("  - Inspect host disk read latency")
	diskReadApi, errMsg := i.GenPrometheusAPIPrefix(`(rate(node_disk_read_time_seconds_total{}[5m])/ rate(node_disk_reads_completed_total{}[5m]))`, i.startTime, i.endTime)
	if errMsg != nil {
		return nil, nil, errMsg
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			var bs strings.Builder
			avgResp, err := request.Request(request.DefaultRequestMethodGet, diskReadApi, nil, "", "")
			if err != nil {
				return err
			}

			avgVal, err := i.GetPromRequestAvgDiskValueByMetric("host disk avg read latency", avgResp)
			if err != nil {
				return err
			}

			for inst, devices := range avgVal {
				var deviceInfos []string
				for d, val := range devices {
					// seconds -> ms
					if val.Mul(decimalMs).LessThan(decimal.NewFromFloat(DefaultHostDiskReadLatencyUnderline * 1000)) {
						deviceInfos = append(deviceInfos, fmt.Sprintf("- %s", d))
					}
				}

				if len(deviceInfos) > 0 {
					bs.WriteString("检查以下主机磁盘平均读延迟超过 10ms:\n")
					instSli := strings.Split(inst, ":")
					bs.WriteString(strings.Join(deviceInfos, "\n"))
					sysConfigOutputs[instSli[0]] = append(sysConfigOutputs[instSli[0]], bs.String())
				}
			}
			return nil
		},
	); err != nil {
		return nil, nil, err
	}
	/*
		1、swap
		2、THP
		3、deployUser uid and gid
		4、deployUser password strategy
		5、ntpd / chronyd
		6、sysctl.conf
		7、limits.conf
	*/
	var (
		inspTasks []*task.StepDisplay
	)
	for _, host := range i.topo.GetClusterTopologyHostIps() {
		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(host, `(command -v swapon > /dev/null && (swapon --show | grep -q . && echo "active" || echo "disabled")) || (cat /proc/swaps >/dev/null | tail -n +2 | grep -q . && echo "active" || echo "disabled")`, fmt.Sprintf("%s_swap", host), true).
			Shell(host, `cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "not found thp"`, fmt.Sprintf("%s_thp", host), true).
			Shell(host, `grep -i AnonHugePages /proc/meminfo | awk "{print \$2 \" \" \$3}"`, fmt.Sprintf("%s_anon_hugpages", host), true).
			Shell(host, "grubby --info `grubby --default-kernel 2>/dev/null` 2>/dev/null || echo 'N/A'", fmt.Sprintf("%s_kernel_version", host), true).
			Shell(host, fmt.Sprintf(`id -u %s 2>/dev/null || echo "no"`, i.topo.ClusterMeta.DeployUser), fmt.Sprintf("%s_uid", host), true).
			Shell(host, fmt.Sprintf(`id -g %s 2>/dev/null || echo "no"`, i.topo.ClusterMeta.DeployUser), fmt.Sprintf("%s_gid", host), true).
			Shell(host, fmt.Sprintf(`(chage -l %s | grep "Maximum number of days between password change" | awk -F: "{print \$2}" | tr -d " ")`, i.topo.ClusterMeta.DeployUser), fmt.Sprintf("%s_passwd", host), true).
			Shell(host, `if systemctl is-active --quiet ntpd; then ntpstat 2>/dev/null; elif systemctl is-active --quiet chronyd; then chronyc tracking | grep "Leap status" || true; fi`, fmt.Sprintf("%s_time", host), true).
			Shell(host, `sysctl -p | grep -E "fs.file-max|net.core.somaxconn|net.ipv4.tcp_tw_recycle|net.ipv4.tcp_syncookies|vm.overcommit_memory|vm.min_free_kbytes"`, fmt.Sprintf("%s_sysctl", host), true).
			Shell(host, `grep -vE "^\s*#|^\s*$" /etc/security/limits.conf`, fmt.Sprintf("%s_limit", host), true).
			BuildAsStep(fmt.Sprintf("    - Inspect machine %s system config", host))

		inspTasks = append(inspTasks, tf)
	}

	if len(tikvDirPointTasks) > 0 || len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger)

		if len(tikvDirPointTasks) > 0 {
			t.ParallelStep("  - Inspect tikv component data mount points", false, tikvDirPointTasks...)
		}
		if len(inspTasks) > 0 {
			t.ParallelStep("  - Inspect deploy machine system config", false, inspTasks...)
		}

		if err := t.Build().Execute(ctx); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch machine system config, error detail: %v", err)
		}

		for host, points := range tikvDirMountPoints {
			var bs strings.Builder

			for seq, point := range points {
				stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_%d", host, seq))
				if !ok {
					return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_%d", host, seq))
				}
				output := strings.Trim(string(stdout), "\n")
				if output == "no" {
					bs.WriteString(fmt.Sprintf("- 挂载点 [%s] 未挂载", point))
				} else {
					outputSli := strings.Split(output, "   ")
					if outputSli[0] != "ext4" {
						bs.WriteString(fmt.Sprintf("- 挂载点 [%s] 文件系统类型错误: %s (应为 ext4)", point, outputSli[0]))
					}

					if !strings.Contains(outputSli[1], "nodelalloc") || !strings.Contains(outputSli[1], "noatime") {
						bs.WriteString(fmt.Sprintf("- 挂载点 [%s] 不包含 nodelalloc 或 noatime 参数，挂载参数错误: %s (应为 ext4)", point, outputSli[1]))
					}
				}
			}
			if bs.String() != "" {
				sysConfigOutputs[host] = append(sysConfigOutputs[host], "检查磁盘挂载参数:")
				sysConfigOutputs[host] = append(sysConfigOutputs[host], bs.String())
			}
		}

		for _, host := range i.topo.GetClusterTopologyHostIps() {
			var bs strings.Builder

			// shell output
			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_swap", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_swap", host))
			}
			output := strings.Trim(string(stdout), "\n")

			if output != "disabled" {
				bs.WriteString(fmt.Sprintf("检查 swap 是否关闭 | swap 未关闭: %s\n", output))
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_thp", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_thp", host))
			}
			thp_output := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_anon_hugpages", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_anon_hugpages", host))
			}
			amon_huge_output := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_kernel_version", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_kernel_version", host))
			}
			kernel_version_output := strings.Trim(string(stdout), "\n")

			if !strings.Contains(thp_output, "[never]") || amon_huge_output != "0 kB" || !strings.Contains(kernel_version_output, "transparent_hugepage=never") {
				bs.WriteString(fmt.Sprintf("检查透明大页是否关闭 | 透明大页未完全禁用:\nTHP_STATUS=%s, AnonHugePages=%s, grub=%s\n", thp_output, amon_huge_output, kernel_version_output))
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_uid", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_uid", host))
			}
			uid_output := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_gid", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_gid", host))
			}
			gid_output := strings.Trim(string(stdout), "\n")

			if uid_output == "no" || gid_output == "no" {
				bs.WriteString(fmt.Sprintf("检查 %s 用户 ID 和组 ID 是否一致 | 部署用户不存在\n", i.topo.ClusterMeta.DeployUser))
			}
			if uid_output != gid_output {
				bs.WriteString(fmt.Sprintf("检查 %s 用户 ID 和组 ID 是否一致 | 部署用户 UID=%s, GID=%s 不一致\n", i.topo.ClusterMeta.DeployUser, uid_output, gid_output))
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_passwd", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_passwd", host))
			}
			passwd := strings.Trim(string(stdout), "\n")
			var (
				expired int64
				err     error
			)
			if passwd == "" {
				expired = 0
			} else {
				expired, err = strconv.ParseInt(passwd, 10, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("parse user for %s password expired failed: %v", i.topo.ClusterMeta.DeployUser, err)
				}
			}
			if expired <= 9999 {
				bs.WriteString(fmt.Sprintf("部署用户 [%s] 系统密码是否不过期|密码有效期不足 9999 天，当前为 %d 天\n", i.topo.ClusterMeta.DeployUser, expired))
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_time", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_time", host))
			}
			time_ouput := strings.Trim(string(stdout), "\n")

			if !strings.Contains(time_ouput, "synchronised to NTP server") && !strings.Contains(time_ouput, "Normal") {
				bs.WriteString("时间同步是否正常 | NTP 未正常同步 OR Chrony 状态异常\n")
			} else {
				bs.WriteString("时间同步是否正常 | 未检测到 NTP 或 Chrony 服务运行\n")
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_sysctl", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_sysctl", host))
			}
			sysctlSli := strings.Split(string(stdout), "\n")

			sysctlNows := make(map[string]int64)
			for _, str := range sysctlSli {
				// skip "" records
				if strings.EqualFold(str, "") {
					continue
				}
				paramValSli := strings.Fields(str)

				param := paramValSli[0]
				val := paramValSli[2]
				paseInt, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("parse param [%s] value [%s] failed: %v", param, val, err)
				}
				sysctlNows[param] = paseInt
			}

			var records []string
			for p, v := range DefaultInspSysctlConfigItems() {
				val, ok := sysctlNows[p]
				if ok {
					if val != v {
						records = append(records, fmt.Sprintf("- %s 期望值 %d，实际值 %d", p, v, val))
					}
				} else {
					records = append(records, fmt.Sprintf("- %s 未设置，期望值 %d", p, v))
				}
			}
			if len(records) > 0 {
				bs.WriteString("检查 /etc/sysctl.conf 参数:\n")
				bs.WriteString(strings.Join(records, "\n"))
			}

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_limit", host))
			if !ok {
				return nil, nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_limit", host))
			}

			scanner := bufio.NewScanner(strings.NewReader(string(stdout)))

			limitsMap := make(map[string]int)

			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}

				fields := strings.Fields(line)
				if len(fields) < 4 {
					continue
				}

				key := fmt.Sprintf("%s %s %s", fields[0], fields[1], fields[2])
				value, err := strconv.Atoi(fields[3])
				if err != nil {
					return nil, nil, fmt.Errorf("skipping invalid value in line [%s] failed: %s", line, err)
				}

				limitsMap[key] = value
			}

			// The check requirement is */deployUser must meet 1 of the following
			deployUserLimits := make(map[string]int)
			allUserLimits := make(map[string]int)
			for k, v := range DefaultInspLimitsParamsConfigItems() {
				deployUserLimits[fmt.Sprintf("%s %s", i.topo.ClusterMeta.DeployUser, strings.ReplaceAll(k, " ", " "))] = v
				allUserLimits[fmt.Sprintf("* %s", strings.ReplaceAll(k, " ", " "))] = v
			}

			// prioritize checking * all user parameter settings. If they do not exist or the parameter values ​​do not meet the requirements, those that do not meet the requirements will be transferred to the deployment user parameter setting check
			allNoExpxect := make(map[string]int)
			for k, v := range allUserLimits {
				val, ok := limitsMap[k]
				if (ok && v != val) || !ok {
					keys := strings.Split(k, " ")
					depKey := fmt.Sprintf("%s %s %s", i.topo.ClusterMeta.DeployUser, keys[1], keys[2])
					allNoExpxect[depKey] = deployUserLimits[depKey]
				}
			}

			if len(allNoExpxect) > 0 {
				bs.WriteString("检查 /etc/security/limits.conf 参数:\n")
			}

			for k, v := range allNoExpxect {
				val, ok := limitsMap[k]
				if ok {
					if v != val {
						bs.WriteString(fmt.Sprintf("- %s 期望值 %d，实际值 %d\n", k, v, val))
					}
				} else {
					bs.WriteString(fmt.Sprintf("- %s 未设置, 期望值：%d\n", k, v))
				}
			}

			if bs.String() != "" {
				sysConfigOutputs[host] = append(sysConfigOutputs[host], bs.String())
			}
		}
	}

	var sysConfigOutputSli []*SystemConfigOutput
	for k, valStr := range sysConfigOutputs {
		sysConfigOutputSli = append(sysConfigOutputSli, &SystemConfigOutput{
			IpAddress:      k,
			AbnormalDetail: strings.Join(valStr, "\n"),
		})
	}
	return DefaultInspSystemConfigItems(), sysConfigOutputSli, nil
}

func (i *Insepctor) InspSystemCrontab() ([]*SystemCrontab, error) {
	var (
		inspTasks []*task.StepDisplay
		sysCrons  []*SystemCrontab
	)
	for _, host := range i.topo.GetClusterTopologyHostIps() {
		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(host, `crontab -l &>/dev/null && { crontab -l | grep . -q || echo "none"; } || echo "none"`, fmt.Sprintf("%s_current_cron", host), true).
			Shell(host, fmt.Sprintf(`crontab -l -u %s 2>/dev/null || echo "none"`, i.label.ClusterMeta.DeployUser), fmt.Sprintf("%s_deploy_cron", host), true).
			BuildAsStep(fmt.Sprintf("  - Inspect machine %s system crontab", host))

		inspTasks = append(inspTasks, tf)
	}

	if len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger).ParallelStep("+ Inspect machine system crontab", false, inspTasks...).Build()
		if err := t.Execute(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch machine system crontab, error detail: %v", err)
		}

		for _, host := range i.topo.GetClusterTopologyHostIps() {
			var lines []string

			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_current_cron", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_current_cron", host))
			}
			scanner := bufio.NewScanner(bytes.NewReader(stdout))
			for scanner.Scan() {
				if scanner.Text() == "none" {
					lines = append(lines, strings.Trim("N/A", "\n"))
				} else {
					lines = append(lines, strings.Trim(scanner.Text(), "\n"))
				}
			}

			sysCrons = append(sysCrons, &SystemCrontab{
				IpAddress:      host,
				CrontabUser:    i.gOpt.SSHUser,
				CrontabContent: strings.Join(lines, "\n"),
			})

			var newLines []string

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_deploy_cron", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_deploy_cron", host))
			}
			scanner = bufio.NewScanner(bytes.NewReader(stdout))
			for scanner.Scan() {
				if scanner.Text() == "none" {
					newLines = append(newLines, strings.Trim("N/A", "\n"))
				} else {
					newLines = append(newLines, strings.Trim(scanner.Text(), "\n"))
				}
			}

			sysCrons = append(sysCrons, &SystemCrontab{
				IpAddress:      host,
				CrontabUser:    i.label.ClusterMeta.DeployUser,
				CrontabContent: strings.Join(newLines, "\n"),
			})
		}
	}

	return sysCrons, nil
}

func (i *Insepctor) InspSystemDmesg() ([]*SystemDmesg, error) {
	var (
		inspTasks []*task.StepDisplay
		sysDmesgs []*SystemDmesg
	)
	for _, host := range i.topo.GetClusterTopologyHostIps() {
		// Construct the SSH command
		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(host, `dmesg | tail -n 500`, fmt.Sprintf("%s_dmesg", host), true).
			Shell(host, `cat /proc/uptime`, fmt.Sprintf("%s_proc", host), true).
			BuildAsStep(fmt.Sprintf("  - Inspect machine %s system dmesg", host))

		inspTasks = append(inspTasks, tf)
	}

	if len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger).ParallelStep("+ Inspect machine system dmesg", false, inspTasks...).Build()
		if err := t.Execute(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch machine system dmesg, error detail: %v", err)
		}

		// calculate the timestamp 30 days ago
		cutoff := time.Now().Add(-30 * 24 * time.Hour)

		for _, host := range i.topo.GetClusterTopologyHostIps() {
			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_proc", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_proc", host))
			}
			procUptime := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_dmesg", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_dmesg", host))
			}
			var lines []string
			scanner := bufio.NewScanner(bytes.NewReader(stdout))
			for scanner.Scan() {
				lines = append(lines, strings.Trim(scanner.Text(), "\n"))
			}

			if len(lines) == 0 {
				sysDmesgs = append(sysDmesgs, &SystemDmesg{
					IpAddress:      host,
					AbnormalStatus: "正常",
					AbnormalDetail: "N/A",
				})
			} else {
				bootTime, err := getBootTime(procUptime)
				if err != nil {
					return nil, fmt.Errorf("get system boot time failed: %v", err)
				}

				// output the last 5 logs in ascending order
				var newLines []string
				if len(lines) <= 5 {
					newLines = lines
				} else {
					newLines = append(newLines, lines[len(lines)-5:]...)
				}
				var outputMsg []string
				for _, line := range newLines {
					if ts, msg, err := parseDmesgLine(line, bootTime); err == nil {
						if ts.After(cutoff) {
							outputMsg = append(outputMsg, fmt.Sprintf("[%s] %s", ts.Format("2006-01-02 15:04:05"), msg))
						}
					}
				}
				if len(outputMsg) > 0 {
					sysDmesgs = append(sysDmesgs, &SystemDmesg{
						IpAddress:      host,
						AbnormalStatus: "异常",
						AbnormalDetail: strings.Join(outputMsg, "\n"),
					})
				} else {
					sysDmesgs = append(sysDmesgs, &SystemDmesg{
						IpAddress:      host,
						AbnormalStatus: "正常",
						AbnormalDetail: "N/A",
					})
				}
			}
		}
	}

	return sysDmesgs, nil
}

func parseDmesgLine(line string, bootTime time.Time) (time.Time, string, error) {
	if parts := strings.SplitN(line, "] ", 2); len(parts) == 2 {
		timeStr := strings.Trim(parts[0], "[")
		timeFloat, err := strconv.ParseFloat(timeStr, 64)
		if err != nil {
			return time.Time{}, "", err
		}
		return bootTime.Add(time.Duration(timeFloat * float64(time.Second))), parts[1], nil
	}
	return time.Time{}, "", fmt.Errorf("invalid format")
}

func getBootTime(data string) (time.Time, error) {
	uptime, _ := strconv.ParseFloat(data, 64)
	return time.Now().Add(-time.Duration(uptime * float64(time.Second))), nil
}

func (i *Insepctor) InspDatabaseErrorCount() ([]*DatabaseErrorCount, error) {
	var (
		dec       []*DatabaseErrorCount
		inspTasks []*task.StepDisplay
	)
	tidbInsts, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNameTiDB)
	if err != nil {
		return nil, err
	}

	for _, inst := range tidbInsts {
		logFile := fmt.Sprintf("%s/log/tidb.log", inst.DeployDir)

		tf := task.NewBuilder(i.logger).
			SSHKeySet(filepath.Join(i.deployUserSshDir, "id_rsa"), filepath.Join(i.deployUserSshDir, "id_rsa.pub")).
			UserSSH(
				inst.Host,
				i.gOpt.SSHPort,
				i.topo.ClusterMeta.DeployUser,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				executor.SSHTypeBuiltin,
			).
			Shell(inst.Host, fmt.Sprintf(`head -1 "%s" | sed -E "s/^\[([^]]+)\].*/\1/"`, logFile), fmt.Sprintf("%s_head", inst.Host), true).
			Shell(inst.Host, fmt.Sprintf(`tail -1 "%s" | sed -E "s/^\[([^]]+)\].*/\1/"`, logFile), fmt.Sprintf("%s_bootom", inst.Host), true).
			Shell(inst.Host, fmt.Sprintf(`grep -c "\\[ERROR\\]" "%s"`, logFile), fmt.Sprintf("%s_error", inst.Host), true).
			BuildAsStep(fmt.Sprintf("  - Inspect tidb instance [%s] log", inst.ID))

		inspTasks = append(inspTasks, tf)
	}

	if len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger).ParallelStep("+ Inspect tidb instance log", false, inspTasks...).Build()
		if err := t.Execute(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch tidb instance log, error detail: %v", err)
		}

		for _, inst := range tidbInsts {
			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_head", inst.Host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_head", inst.Host))
			}
			startTime := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_bootom", inst.Host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_bootom", inst.Host))
			}
			endTime := strings.Trim(string(stdout), "\n")

			stdout, _, ok = ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_error", inst.Host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_error", inst.Host))
			}
			errCounts := strings.Trim(string(stdout), "\n")

			if errCounts != "" {
				dec = append(dec, &DatabaseErrorCount{
					InstAddress: inst.ID,
					Component:   inst.ComponentName,
					ErrorCount: fmt.Sprintf("分析的日志： %s\n日志的时间范围：%s - %s\n[ERROR] 类型报错有 %s 条",
						fmt.Sprintf("%s/log/tidb.log", inst.DeployDir), startTime, endTime, errCounts),
				})
			}
		}
	}

	return dec, nil
}

func (i *Insepctor) InspDatabaseSchemaSpace() ([]*DatabaseSchemaSpace, error) {
	i.logger.Infof("+ Inspect database schema space distributed")

	db := i.connector.(*mysql.Database)

	_, res, err := db.GeneralQuery(i.ctx, `SELECT table_schema, ROUND(SUM(index_length) / 1024 / 1024 / 1024, 2) AS index_length_GB, ROUND(SUM(data_length) / 1024 / 1024 / 1024, 2) AS data_length_GB, ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 2) AS total_GB FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'mysql') GROUP BY TABLE_SCHEMA`)
	if err != nil {
		return nil, err
	}
	var ds []*DatabaseSchemaSpace
	for _, r := range res {
		ds = append(ds, &DatabaseSchemaSpace{
			SchemaName:   r["table_schema"],
			IndexSpaceGB: r["index_length_GB"],
			DataSpaceGB:  r["data_length_GB"],
			TotalSpaceGB: r["total_GB"],
		})
	}
	return ds, nil
}

func (i *Insepctor) InspDatabaseTableSpaceTop() ([]*DatabaseTableSpaceTop, error) {
	i.logger.Infof("+ Inspect database table space distributed")

	db := i.connector.(*mysql.Database)

	_, res, err := db.GeneralQuery(i.ctx, `	SELECT base.schema_name as schema_name, base.table_name as table_name, base.rows_count as rows_count, cols.column_count as column_count, base.size_GB as size_GB FROM (SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, TABLE_ROWS AS rows_count, ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 / 1024, 2) AS size_GB FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'mysql')) AS base LEFT JOIN (SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COUNT(COLUMN_NAME) AS column_count FROM information_schema.columns GROUP BY TABLE_SCHEMA, TABLE_NAME) AS cols ON base.schema_name = cols.schema_name AND base.table_name = cols.table_name ORDER BY base.size_GB DESC LIMIT 10`)
	if err != nil {
		return nil, err
	}

	var ds []*DatabaseTableSpaceTop
	for _, r := range res {
		ds = append(ds, &DatabaseTableSpaceTop{
			SchemaName:   r["schema_name"],
			TableName:    r["table_name"],
			RowCounts:    r["rows_count"],
			ColumnCounts: r["column_count"],
			TotalSpaceGB: r["size_GB"],
		})
	}
	return ds, nil
}

func (i *Insepctor) InspPerformanceStatisticsByPD() ([]*PerformanceStatisticsByPD, error) {
	i.logger.Infof("+ Inspect performance statistics by pd")

	var (
		psbp      []*PerformanceStatisticsByPD
		inspTasks []*task.StepDisplay
	)
	statusPortMapping := i.topo.GetClusterComponentStatusServicePortMapping()

	for _, host := range i.topo.GetClusterTopologyHostIps() {
		// Construct the SSH command
		sshCmd := `lscpu | grep "^CPU(s):" | awk -F":" "{print \$2}" | xargs || echo "N/A"`

		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(host, sshCmd, fmt.Sprintf("%s_lscpu", host), true).
			BuildAsStep(fmt.Sprintf("  - Inspect machine %s lscpu statistics", host))

		inspTasks = append(inspTasks, tf)
	}

	machineCpu := make(map[string]string)
	if len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger).ParallelStep("+ Inspect machine lscpu statistics", false, inspTasks...).Build()
		if err := t.Execute(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch machine system lscpu statistics, error detail: %v", err)
		}

		for _, host := range i.topo.GetClusterTopologyHostIps() {
			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_lscpu", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_lscpu", host))
			}
			cpu := strings.Trim(string(stdout), "\n")

			machineCpu[host] = cpu
		}
	}

	i.logger.Infof("  - Inspect pd component cpu usage")

	avgApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`avg_over_time(rate(process_cpu_seconds_total{job="pd"}[1m])[%dm:])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`max_over_time(rate(process_cpu_seconds_total{job="pd"}[1m])[%dm:])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}

			avgVal, err := i.GetPromRequestAvgValueByMetric("pd_server avg cpu", avgResp)
			if err != nil {
				return err
			}

			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestMaxValueByMetric("pd_server max cpu", maxResp)
			if err != nil {
				return err
			}
			for inst, avg := range avgVal {
				machineCpu := machineCpu[strings.Split(inst, ":")[0]]

				cpuLimitI, err := strconv.ParseInt(machineCpu, 10, 64)
				if err != nil {
					return fmt.Errorf("pd machine cpu parse value [%s] failed: %v", machineCpu, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByPD{
						PDInstance:      statusPortMapping[inst],
						MonitoringItems: "cpu usage",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      machineCpu,
						SuggestValue:    "应低于 80% * cpu limit",
						Comment:         "读取服务器 vcore 数量",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect pd component region heartbeat latency")

	regionApi, err := i.GenPrometheusAPIPrefix(`histogram_quantile(0.99, sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_bucket{}[1m])) by (address, le))`, i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			regionResp, err := request.Request(request.DefaultRequestMethodGet, regionApi, nil, "", "")
			if err != nil {
				return err
			}

			avgRegionVal, err := i.GetPromRequestAvgValueByMetric("pd_server avg region heartbeat handle duration", regionResp, true)
			if err != nil {
				return err
			}

			maxRegionVal, err := i.GetPromRequestMaxValueByMetric("pd_server max region heartbeat handle duration", regionResp, true)
			if err != nil {
				return err
			}

			suggest := fmt.Sprintf("%2.fms", DefaultPdComponentRegionHeartbeatHandleLatencyUnderline*1000)
			for inst, avg := range avgRegionVal {
				if avg.GreaterThan(decimal.NewFromFloat(DefaultPdComponentRegionHeartbeatHandleLatencyUnderline)) {
					psbp = append(psbp, &PerformanceStatisticsByPD{
						PDInstance:      statusPortMapping[inst],
						MonitoringItems: "99% region heartbeat handle latency",
						AvgMetrics:      fmt.Sprintf(`%vs`, avg.Round(2).String()),
						MaxMetrics:      fmt.Sprintf(`%vs`, maxRegionVal[inst].Round(2).String()),
						ParamValue:      suggest,
						SuggestValue:    fmt.Sprintf("应低于 %v", suggest),
						Comment:         "经验延迟值",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect pd component handle request latency")

	requestApi, err := i.GenPrometheusAPIPrefix(`histogram_quantile(0.99, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{}[30s])) by (type, le))`, i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			requestResp, err := request.Request(request.DefaultRequestMethodGet, requestApi, nil, "", "")
			if err != nil {
				return err
			}

			avgRequestVal, err := i.GetPromRequestAvgValueByNonMetric("pd_server avg handle request duration", requestResp)
			if err != nil {
				return err
			}

			maxRequestVal, err := i.GetPromRequestMaxValueByNonMetric("pd_server max handle request duration", requestResp)
			if err != nil {
				return err
			}

			suggest := fmt.Sprintf("%2.fms", DefaultPdComponentHandleReuqestDurationUnderline*1000)

			if avgRequestVal.GreaterThan(decimal.NewFromFloat(DefaultPdComponentHandleReuqestDurationUnderline)) {
				psbp = append(psbp, &PerformanceStatisticsByPD{
					PDInstance:      i.topo.GetClusterComponentPDComponenetLeaderServiceAddress(),
					MonitoringItems: "99% handle request duration",
					AvgMetrics:      fmt.Sprintf(`%vs`, avgRequestVal.Round(2).String()),
					MaxMetrics:      fmt.Sprintf(`%vs`, maxRequestVal.Round(2).String()),
					ParamValue:      suggest,
					SuggestValue:    fmt.Sprintf("应低于 %v", suggest),
					Comment:         "经验延迟值",
				})
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect pd component wal fsync latency")

	walApi, err := i.GenPrometheusAPIPrefix(`histogram_quantile(0.99, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket{}[5m])) by (instance, le))`, i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			walResp, err := request.Request(request.DefaultRequestMethodGet, walApi, nil, "", "")
			if err != nil {
				return err
			}

			avgWalVal, err := i.GetPromRequestAvgValueByMetric("pd_server avg wal fsync duration", walResp)
			if err != nil {
				return err
			}

			maxWalVal, err := i.GetPromRequestMaxValueByMetric("pd_server max wal fsync duration", walResp)
			if err != nil {
				return err
			}

			suggest := fmt.Sprintf("%2.fms", DefaultPdComponentWalFsyncDurationUnderline*1000)
			for inst, avg := range avgWalVal {
				if avg.GreaterThan(decimal.NewFromFloat(DefaultPdComponentWalFsyncDurationUnderline)) {
					psbp = append(psbp, &PerformanceStatisticsByPD{
						PDInstance:      statusPortMapping[inst],
						MonitoringItems: "99% WAL fsync duration",
						AvgMetrics:      fmt.Sprintf(`%vs`, avg.Round(2).String()),
						MaxMetrics:      fmt.Sprintf(`%vs`, maxWalVal[inst].Round(2).String()),
						ParamValue:      suggest,
						SuggestValue:    fmt.Sprintf("应低于 %v", suggest),
						Comment:         "经验延迟值",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return psbp, nil
}

func (i *Insepctor) InspPerformanceStatisticsByTiDB() ([]*PerformanceStatisticsByTiDB, error) {
	i.logger.Infof("+ Inspect performance statistics by tidb")

	var (
		psbp      []*PerformanceStatisticsByTiDB
		inspTasks []*task.StepDisplay
	)

	database := i.connector.(*mysql.Database)

	statusPortMapping := i.topo.GetClusterComponentStatusServicePortMapping()

	maxProcs := make(map[string]int64)

	_, res, err := database.GeneralQuery(i.ctx, `show config where name='performance.max-procs'`)
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		val, err := strconv.ParseInt(r["Value"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("performance statistics by tidb parse maxProcs [%s] failed: %v", r["Value"], err)
		}
		maxProcs[r["Instance"]] = val
	}

	_, res, err = database.GeneralQuery(i.ctx, `select MEMORY_LIMIT/1024/1024/1024 AS GB from information_schema.MEMORY_USAGE`)
	if err != nil {
		return nil, err
	}
	memoryLimit, err := strconv.ParseFloat(res[0]["GB"], 64)
	if err != nil {
		return nil, fmt.Errorf("performance statistics by tidb parse memory [%s] failed: %v", res[0]["GB"], err)
	}

	for _, host := range i.topo.GetClusterTopologyHostIps() {
		// Construct the SSH command
		sshCmd := `lscpu | grep "^CPU(s):" | awk -F":" "{print \$2}" | xargs || echo "N/A"`

		tf := task.NewBuilder(i.logger).
			RootSSH(
				host,
				i.gOpt.SSHPort,
				i.gOpt.SSHUser,
				i.ssh.Password,
				i.ssh.IdentityFile,
				i.ssh.IdentityFilePassphrase,
				i.gOpt.SSHTimeout,
				i.gOpt.OptTimeout,
				i.gOpt.SSHProxyHost,
				i.gOpt.SSHProxyPort,
				i.gOpt.SSHProxyUser,
				i.proxy.Password,
				i.proxy.IdentityFile,
				i.proxy.IdentityFilePassphrase,
				i.gOpt.SSHProxyTimeout,
				i.gOpt.SSHType,
				true,
			).
			Shell(host, sshCmd, fmt.Sprintf("%s_lscpu", host), true).
			BuildAsStep(fmt.Sprintf("  - Inspect machine %s lscpu statistics", host))

		inspTasks = append(inspTasks, tf)
	}

	machineCpu := make(map[string]string)
	if len(inspTasks) > 0 {
		ctx := ctxt.New(
			i.ctx,
			i.gOpt.Concurrency,
			i.logger,
		)

		t := task.NewBuilder(i.logger).ParallelStep("+ Inspect machine lscpu statistics", false, inspTasks...).Build()
		if err := t.Execute(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch machine system lscpu statistics, error detail: %v", err)
		}

		for _, host := range i.topo.GetClusterTopologyHostIps() {
			stdout, _, ok := ctxt.GetInner(ctx).GetOutputs(fmt.Sprintf("%s_lscpu", host))
			if !ok {
				return nil, fmt.Errorf("no check results found for %s", fmt.Sprintf("%s_lscpu", host))
			}
			cpu := strings.Trim(string(stdout), "\n")

			machineCpu[host] = cpu
		}
	}

	i.logger.Infof("  - Inspect tidb component cpu usage")

	avgApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`avg_over_time(rate(process_cpu_seconds_total{job="tidb"}[1m])[%dm:])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`max_over_time(rate(process_cpu_seconds_total{job="tidb"}[1m])[%dm:])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tidb_server avg cpu", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestMaxValueByMetric("tidb_server max cpu", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				var (
					paramVal int64
					paramStr string
					remark   string
				)
				val, ok := maxProcs[statusPortMapping[inst]]
				if ok {
					paramVal = val
				}

				if paramVal == 0 {
					paramStr = machineCpu[strings.Split(inst, ":")[0]]
					remark = "读取服务器 vcore 数量"
				} else {
					paramStr = strconv.FormatInt(paramVal, 10)
					remark = "读取 performance.max-procs 的值"
				}

				cpuLimitI, err := strconv.ParseInt(paramStr, 10, 64)
				if err != nil {
					return fmt.Errorf("tidb machine cpu parse value [%s] failed: %v", paramStr, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiDB{
						TiDBInstance:    statusPortMapping[inst],
						MonitoringItems: "cpu usage",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      paramStr,
						SuggestValue:    "应低于 80% * cpu limit",
						Comment:         remark,
					})
				}

			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tidb component memory usage")

	avgApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`avg_over_time(process_resident_memory_bytes{job="tidb"}[%dm])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`max_over_time(process_resident_memory_bytes{job="tidb"}[%dm])`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tidb_server avg memory", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestMaxValueByMetric("tidb_server max memory", maxResp)
			if err != nil {
				return err
			}

			decimalN := decimal.NewFromInt(1024 * 1024 * 1024)
			for inst, avg := range avgVal {
				if maxVal[inst].DivRound(decimalN, 2).GreaterThan(decimal.NewFromFloat(float64(memoryLimit) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiDB{
						TiDBInstance:    statusPortMapping[inst],
						MonitoringItems: "memory usage",
						AvgMetrics:      avg.DivRound(decimalN, 2).String(),
						MaxMetrics:      maxVal[inst].DivRound(decimalN, 2).String(),
						ParamValue:      fmt.Sprintf("%2.f", memoryLimit),
						SuggestValue:    "应低于 80% * memory limit",
						Comment:         "读取 information_schema.MEMORY_USAGE 的 MEMORY_LIMIT 字段",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tidb component token wait duration")

	waitApi, err := i.GenPrometheusAPIPrefix(`histogram_quantile(0.99, sum(rate(tidb_tikvclient_batch_executor_token_wait_duration_bucket{}[1m])) by (instance,le))`, i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			waitResp, err := request.Request(request.DefaultRequestMethodGet, waitApi, nil, "", "")
			if err != nil {
				return err
			}
			waitAvgVal, err := i.GetPromRequestAvgValueByMetric("tidb_server token wait avg duration", waitResp)
			if err != nil {
				return err
			}

			waitMaxVal, err := i.GetPromRequestMaxValueByMetric("tidb_server token wait max duration", waitResp)
			if err != nil {
				return err
			}

			// nanoseconds to milliseconds
			decimalMs := decimal.NewFromInt(1000000)
			suggest := fmt.Sprintf("%2.fms", DefaultTiDBComponentCommitTokenWaitDurationUnderline*1000)
			for inst, avg := range waitAvgVal {
				if avg.DivRound(decimalMs, 2).GreaterThan(decimal.NewFromFloat(DefaultTiDBComponentCommitTokenWaitDurationUnderline)) {
					psbp = append(psbp, &PerformanceStatisticsByTiDB{
						TiDBInstance:    statusPortMapping[inst],
						MonitoringItems: "commit token wait duration",
						AvgMetrics:      fmt.Sprintf(`%vs`, avg.Round(2).String()),
						MaxMetrics:      fmt.Sprintf(`%vs`, waitMaxVal[inst].Round(2).String()),
						ParamValue:      suggest,
						SuggestValue:    fmt.Sprintf("应低于 %v", suggest),
						Comment:         "经验延迟值",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return psbp, nil
}

func (i *Insepctor) InspPerformanceStatisticsByTiKV() ([]*PerformanceStatisticsByTiKV, error) {
	i.logger.Infof("+ Inspect performance statistics by tikv")

	var (
		psbp []*PerformanceStatisticsByTiKV
	)

	database := i.connector.(*mysql.Database)

	statusPortMapping := i.topo.GetClusterComponentStatusServicePortMapping()

	_, res, err := database.GeneralQuery(i.ctx, "show config where `type` = 'tikv' and name in ('server.grpc-concurrency','storage.scheduler-worker-pool-size','readpool.unified.max-thread-count','raftstore.store-pool-size','raftstore.apply-pool-size')")
	if err != nil {
		return nil, err
	}

	cfgParamVals := make(map[string]map[string]string)
	for _, r := range res {
		vals, exist := cfgParamVals[r["Instance"]]
		if exist {
			vals[r["Name"]] = r["Value"]
			cfgParamVals[r["Instance"]] = vals
		} else {
			val := make(map[string]string)
			val[r["Name"]] = r["Value"]
			cfgParamVals[r["Instance"]] = val
		}
	}

	i.logger.Infof("  - Inspect tikv component grpc cpu usage")

	avgApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"grpc_server.*"}[%dm]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err := i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)(max_over_time(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"grpc_server.*"}[1m])[%dm:]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg grpc poll cpu", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestAvgValueByMetric("tikv_server max grpc poll cpu", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				defaultVal := cfgParamVals[statusPortMapping[inst]]["server.grpc-concurrency"]
				cpuLimitI, err := strconv.ParseInt(defaultVal, 10, 64)
				if err != nil {
					return fmt.Errorf("tikv machine grpc cpu parse value [%s] failed: %v", defaultVal, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "grpc poll cpu",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      defaultVal,
						SuggestValue:    "应低于 80% * server.grpc-concurrency",
						Comment:         "读取 server.grpc-concurrency 参数配置值",
					})
				}

			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tikv component scheduler pool usage")

	avgApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"sched_.*"}[%dm]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( max_over_time(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"sched_.*"}[1m])[%dm:]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg scheduler pool size", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestAvgValueByMetric("tikv_server max scheduler pool size", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				defaultVal := cfgParamVals[statusPortMapping[inst]]["storage.scheduler-worker-pool-size"]
				cpuLimitI, err := strconv.ParseInt(defaultVal, 10, 64)
				if err != nil {
					return fmt.Errorf("tikv machine scheduler cpu parse value [%s] failed: %v", defaultVal, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "scheduler worker cpu",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      defaultVal,
						SuggestValue:    "应低于 80% * storage.scheduler-worker-pool-size",
						Comment:         "读取 storage.scheduler-worker-pool-size 参数配置值",
					})
				}

			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tikv component unified pool usage")

	avgApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"unified_read_po.*"}[%dm]) )`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( max_over_time(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"unified_read_po.*"}[1m])[%dm:]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg unified pool size", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestAvgValueByMetric("tikv_server max unified pool size", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				defaultVal := cfgParamVals[statusPortMapping[inst]]["readpool.unified.max-thread-count"]
				cpuLimitI, err := strconv.ParseInt(defaultVal, 10, 64)
				if err != nil {
					return fmt.Errorf("tikv machine unified max thread parse value [%s] failed: %v", defaultVal, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "unified read pool cpu",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      defaultVal,
						SuggestValue:    "应低于 80% * readpool.unified.max-thread-count",
						Comment:         "读取 readpool.unified.max-thread-count 参数配置值",
					})
				}

			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tikv component raft store pool usage")

	avgApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"raftstore_.*"}[%dm]) )`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( max_over_time(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"raftstore_.*"}[1m])[%dm:]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg raft store cpu", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestAvgValueByMetric("tikv_server max raft store cpu", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				defaultVal := cfgParamVals[statusPortMapping[inst]]["raftstore.store-pool-size"]
				cpuLimitI, err := strconv.ParseInt(defaultVal, 10, 64)
				if err != nil {
					return fmt.Errorf("tikv machine raft store cpu parse value [%s] failed: %v", defaultVal, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "raft store cpu",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      defaultVal,
						SuggestValue:    "应低于 80% * raftstore.store-pool-size",
						Comment:         "读取 raftstore.store-pool-size 参数配置值",
					})
				}

			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tikv component raft apply pool usage")

	avgApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"apply_.*"}[%dm]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}
	maxApi, err = i.GenPrometheusAPIPrefix(fmt.Sprintf(`sum by(instance)( max_over_time(rate(tikv_thread_cpu_seconds_total{job="tikv", name=~"apply_.*"}[1m])[%dm:]))`, i.inspConfig.WindowMinutes), i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			avgResp, err := request.Request(request.DefaultRequestMethodGet, avgApi, nil, "", "")
			if err != nil {
				return err
			}
			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg raft apply cpu", avgResp)
			if err != nil {
				return err
			}
			maxResp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}
			maxVal, err := i.GetPromRequestAvgValueByMetric("tikv_server max raft apply cpu", maxResp)
			if err != nil {
				return err
			}

			for inst, avg := range avgVal {
				defaultVal := cfgParamVals[statusPortMapping[inst]]["raftstore.apply-pool-size"]
				cpuLimitI, err := strconv.ParseInt(defaultVal, 10, 64)
				if err != nil {
					return fmt.Errorf("tikv machine raft apply cpu parse value [%s] failed: %v", defaultVal, err)
				}

				if maxVal[inst].GreaterThan(decimal.NewFromFloat(float64(cpuLimitI) * 0.8)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "async apply cpu",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avg.String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, maxVal[inst].String()),
						ParamValue:      defaultVal,
						SuggestValue:    "应低于 80% * raftstore.apply-pool-size",
						Comment:         "读取 raftstore.apply-pool-size 参数配置值",
					})
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	i.logger.Infof("  - Inspect tikv component scheduler discard ratio")

	maxApi, err = i.GenPrometheusAPIPrefix(`sum(tikv_scheduler_discard_ratio{}) by (instance) / 10000000`, i.startTime, i.endTime)
	if err != nil {
		return nil, err
	}

	if err := request.Retry(
		&request.RetryConfig{
			MaxRetries: request.DefaultRequestErrorMaxRetries,
			Delay:      request.DefaultRequestErrorRereyDelay,
		},
		func(err error) bool {
			return true
		},
		func() error {
			resp, err := request.Request(request.DefaultRequestMethodGet, maxApi, nil, "", "")
			if err != nil {
				return err
			}

			avgVal, err := i.GetPromRequestAvgValueByMetric("tikv_server avg scheduler discard ratio", resp)
			if err != nil {
				return err
			}

			maxVal, err := i.GetPromRequestMaxValueByMetric("tikv_server max scheduler discard ratio", resp)
			if err != nil {
				return err
			}

			discard := strconv.Itoa(DefaultTiKVComponentSchedulerDiscardRatioUnderline)
			for inst, val := range maxVal {
				if val.GreaterThan(decimal.NewFromFloat(0)) {
					psbp = append(psbp, &PerformanceStatisticsByTiKV{
						TiKVInstance:    statusPortMapping[inst],
						MonitoringItems: "scheduler discard ratio",
						AvgMetrics:      fmt.Sprintf(`%v%%`, avgVal[inst].Round(2).String()),
						MaxMetrics:      fmt.Sprintf(`%v%%`, val.Round(2).String()),
						ParamValue:      discard,
						SuggestValue:    fmt.Sprintf("应等于 %v，> %v 说明存在流控", discard, discard),
						Comment:         "人为判断是否合理",
					})
				}
			}

			return nil
		},
	); err != nil {
		return nil, err
	}

	return psbp, nil
}

func (i *Insepctor) InspSqlOrderedByElapsedTime() ([]*SqlOrderedByElapsedTime, error) {
	i.logger.Infof("+ Inspect sql ordered by elapsed time")

	db := i.connector.(*mysql.Database)

	_, res, err := db.GeneralQuery(i.ctx, fmt.Sprintf(`SELECT
  COALESCE(SUM(sum_latency)/1000000000, 0) AS SUM_LATENCY
FROM information_schema.cluster_statements_summary_history a
WHERE a.summary_begin_time <= NOW()
  AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
  AND a.query_sample_text NOT LIKE '%%/*+ monitoring */%%'`, i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	sumLatency, err := decimal.NewFromString(res[0]["SUM_LATENCY"])
	if err != nil {
		return nil, err
	}

	if sumLatency.Equal(decimal.NewFromInt(0)) {
		return nil, nil
	}

	_, res, err = db.GeneralQuery(i.ctx, fmt.Sprintf(`/*+ monitoring */ SELECT
       total_latency_s,
       total_execs,
       avg_latency_s,
       min_latency_s,
       max_latency_s,
       avg_total_keys,
       avg_processed_keys,
       percentage,
       sql_digest,
       sql_text
  FROM (
    SELECT 
         SUM(sum_latency)/1000000000 AS total_latency_s,             -- SQL 总耗时（秒）
         SUM(exec_count)            AS total_execs,                 -- SQL 总执行次数
         AVG(avg_latency)/1000000000 AS avg_latency_s,              -- 平均执行耗时（秒）
         MIN(min_latency)/1000000000 AS min_latency_s,              -- 最小执行耗时（秒）
         MAX(max_latency)/1000000000 AS max_latency_s,              -- 最大执行耗时（秒）
         ROUND(AVG(avg_total_keys)) AS avg_total_keys,              -- 平均扫描 keys 数量
         ROUND(AVG(avg_processed_keys)) AS avg_processed_keys,      -- 平均处理 keys 数量
         ROUND(SUM(sum_latency)/1000000000 / %v * 100, 2) AS percentage, -- 总耗时占比
         DIGEST                     AS sql_digest,                 -- SQL Digest
         MIN(QUERY_SAMPLE_TEXT)     AS sql_text                    -- 示例 SQL 文本
      FROM information_schema.cluster_statements_summary_history a
     WHERE a.summary_begin_time <= NOW()
       AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
       AND a.query_sample_text NOT LIKE '%%/*+ monitoring */%%'
     GROUP BY DIGEST
  ) aaa
  ORDER BY total_latency_s DESC
  LIMIT 10`, sumLatency.String(), i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	var st []*SqlOrderedByElapsedTime
	for _, r := range res {
		st = append(st, &SqlOrderedByElapsedTime{
			ElapsedTime:       r["total_latency_s"],
			Executions:        r["total_execs"],
			ElapPerExec:       r["avg_latency_s"],
			MinQueryTime:      r["min_latency_s"],
			MaxQueryTime:      r["max_latency_s"],
			AvgTotalKeys:      r["avg_total_keys"],
			AvgProcessedKeys:  r["avg_processed_keys"],
			SqlTimePercentage: r["percentage"],
			SqlDigest:         r["sql_digest"],
			SqlText:           r["sql_text"],
		})
	}
	return st, nil
}

func (i *Insepctor) InspSqlOrderedByComponentCpuTime(checkTidbCpu, checkTikVCpu bool) ([]*SqlOrderedByTiDBCpuTime, []*SqlOrderedByTiKVCpuTime, error) {
	now := time.Now()

	before := now.Add(-30 * time.Minute)

	startSecs := before.Unix()
	endSecs := now.Unix()

	var (
		tidbCpu []*SqlOrderedByTiDBCpuTime
		tikvCpu []*SqlOrderedByTiKVCpuTime
		err     error
	)
	if checkTidbCpu {
		tidbCpu, err = i.InspSqlOrderedByTiDBCpuTime(startSecs, endSecs)
		if err != nil {
			return tidbCpu, tikvCpu, err
		}
	}

	if checkTikVCpu {
		tikvCpu, err = i.InspSqlOrderedByTiKVCpuTime(startSecs, endSecs)
		if err != nil {
			return tidbCpu, tikvCpu, err
		}
	}
	return tidbCpu, tikvCpu, nil
}

func (i *Insepctor) InspSqlOrderedByTiDBCpuTime(startSecs, endSecs int64) ([]*SqlOrderedByTiDBCpuTime, error) {
	i.logger.Infof("+ Inspect sql ordered by tidb cpu time")

	tidbInsts, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNameTiDB)
	if err != nil {
		return nil, err
	}

	g, _ := errgroup.WithContext(i.ctx)
	g.SetLimit(i.gOpt.Concurrency)

	var clusterTopSqls = NewTopSqls()

	for _, inst := range tidbInsts {
		i.logger.Infof("  - Inspect tidb instance [%s] cpu time", inst.ID)

		g.Go(func() error {
			var instTopSql *TopSql
			tidbStatusPort := fmt.Sprintf("%s:%s", inst.Host, strings.Split(inst.Ports, "/")[1])

			req, err := i.GenNgMonitorAPIPrefix(startSecs, endSecs, strings.ToLower(operator.ComponentNameTiDB), tidbStatusPort, 10)
			if err != nil {
				return err
			}

			if err := request.Retry(
				&request.RetryConfig{
					MaxRetries: request.DefaultRequestErrorMaxRetries,
					Delay:      request.DefaultRequestErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					resp, err := request.Request(request.DefaultRequestMethodGet, req, nil, "", "")
					if err != nil {
						return err
					}
					if err := json.Unmarshal(resp, &instTopSql); err != nil {
						return err
					}

					clusterTopSqls.Append(instTopSql.Data...)
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

	var sbt []*SqlOrderedByTiDBCpuTime
	for _, s := range clusterTopSqls.ComputeComponentCpuTime() {
		sbt = append(sbt, &SqlOrderedByTiDBCpuTime{
			CpuTimeSec:        fmt.Sprintf("%2.f", s.CpuTimeSec),
			ExecCountsPerSec:  fmt.Sprintf("%2.f", s.ExecCountPerSec),
			LatencyPerExec:    fmt.Sprintf("%2.f", s.LatencyPerExecSec),
			ScanRecordPerSec:  s.ScanRecordsPerSec,
			ScanIndexesPerSec: s.ScanIndexesPerSec,
			PlanCounts:        s.PlanCounts,
			SqlDigest:         s.SqlDigest,
			SqlText:           s.SqlText,
		})
	}
	return sbt, nil
}

func (i *Insepctor) InspSqlOrderedByTiKVCpuTime(startSecs, endSecs int64) ([]*SqlOrderedByTiKVCpuTime, error) {
	i.logger.Infof("+ Inspect sql ordered by tikv cpu time")

	tikvInsts, err := i.topo.GetClusterTopologyComponentInstances(operator.ComponentNameTiKV)
	if err != nil {
		return nil, err
	}

	g, _ := errgroup.WithContext(i.ctx)
	g.SetLimit(i.gOpt.Concurrency)

	var clusterTopSqls = NewTopSqls()

	for _, inst := range tikvInsts {
		i.logger.Infof("  - Inspect tikv instance [%s] cpu time", inst.ID)
		g.Go(func() error {

			var instTopSql *TopSql
			tikvStatusPort := fmt.Sprintf("%s:%s", inst.Host, strings.Split(inst.Ports, "/")[1])

			req, err := i.GenNgMonitorAPIPrefix(startSecs, endSecs, strings.ToLower(operator.ComponentNameTiKV), tikvStatusPort, 10)
			if err != nil {
				return err
			}

			if err := request.Retry(
				&request.RetryConfig{
					MaxRetries: request.DefaultRequestErrorMaxRetries,
					Delay:      request.DefaultRequestErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					resp, err := request.Request(request.DefaultRequestMethodGet, req, nil, "", "")
					if err != nil {
						return err
					}
					if err := json.Unmarshal(resp, &instTopSql); err != nil {
						return err
					}

					clusterTopSqls.Append(instTopSql.Data...)
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

	var sbt []*SqlOrderedByTiKVCpuTime
	for _, s := range clusterTopSqls.ComputeComponentCpuTime() {
		sbt = append(sbt, &SqlOrderedByTiKVCpuTime{
			CpuTimeSec:        fmt.Sprintf("%2.f", s.CpuTimeSec),
			ExecCountsPerSec:  fmt.Sprintf("%2.f", s.ExecCountPerSec),
			LatencyPerExec:    fmt.Sprintf("%2.f", s.LatencyPerExecSec),
			ScanRecordPerSec:  s.ScanRecordsPerSec,
			ScanIndexesPerSec: s.ScanIndexesPerSec,
			PlanCounts:        s.PlanCounts,
			SqlDigest:         s.SqlDigest,
			SqlText:           s.SqlText,
		})
	}
	return sbt, nil
}

func (i *Insepctor) InspSqlOrderedByExecutions() ([]*SqlOrderedByExecution, error) {
	i.logger.Infof("+ Inspect sql ordered by executions")

	db := i.connector.(*mysql.Database)

	_, res, err := db.GeneralQuery(i.ctx, fmt.Sprintf(`SELECT
  COALESCE(SUM(sum_latency)/1000000000, 0) AS SUM_LATENCY
FROM information_schema.cluster_statements_summary_history a
WHERE a.summary_begin_time <= NOW()
  AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
  AND a.query_sample_text NOT LIKE '%%/*+ monitoring */%%'`, i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	sumLatency, err := decimal.NewFromString(res[0]["SUM_LATENCY"])
	if err != nil {
		return nil, err
	}

	if sumLatency.Equal(decimal.NewFromInt(0)) {
		return nil, nil
	}

	_, res, err = db.GeneralQuery(i.ctx, fmt.Sprintf(`/*+ monitoring */ SELECT
       DENSE_RANK() OVER w AS total_execs_rank,
       aaa.total_execs,
       aaa.avg_latency_s,
       aaa.avg_parse_latency_s,
       aaa.avg_compile_latency_s,
       aaa.min_latency_s,
       aaa.max_latency_s,
       aaa.avg_total_keys,
       aaa.avg_processed_keys,
       aaa.percentage,
       aaa.sql_digest,
       aaa.sql_text
  FROM (
      SELECT
          SUM(exec_count)                  AS total_execs,
          AVG(avg_latency)/1000000000      AS avg_latency_s,
          AVG(avg_parse_latency)/1000000000    AS avg_parse_latency_s,
          AVG(avg_compile_latency)/1000000000  AS avg_compile_latency_s,
          MIN(min_latency)/1000000000      AS min_latency_s,
          MAX(max_latency)/1000000000      AS max_latency_s,
          AVG(avg_total_keys)              AS avg_total_keys,
          AVG(avg_processed_keys)          AS avg_processed_keys,
          ROUND(SUM(sum_latency)/1000000000/%v* 100, 2) AS percentage,
          DIGEST                           AS sql_digest,
          MIN(QUERY_SAMPLE_TEXT)           AS sql_text
        FROM information_schema.cluster_statements_summary_history a
       WHERE a.summary_begin_time <= NOW()
         AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
         AND a.query_sample_text NOT LIKE '%%/*+ monitoring */%%'
       GROUP BY SAMPLE_USER, DIGEST, SCHEMA_NAME
  ) aaa
  WINDOW w AS (ORDER BY total_execs DESC)
  LIMIT 10`, sumLatency.String(), i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	var st []*SqlOrderedByExecution
	for _, r := range res {
		st = append(st, &SqlOrderedByExecution{
			Executions:        r["total_execs"],
			ElapPerExec:       r["avg_latency_s"],
			ParsePerExec:      r["avg_parse_latency_s"],
			CompilePerExec:    r["avg_compile_latency_s"],
			MinQueryTime:      r["min_latency_s"],
			MaxQueryTime:      r["max_latency_s"],
			AvgTotalKeys:      r["avg_total_keys"],
			AvgProcessedKeys:  r["avg_processed_keys"],
			SqlTimePercentage: r["percentage"],
			SqlDigest:         r["sql_digest"],
			SqlText:           r["sql_text"],
		})
	}
	return st, nil
}

func (i *Insepctor) InspSqlOrderedByPlans() ([]*SqlOrderedByPlan, error) {
	i.logger.Infof("+ Inspect sql ordered by plans")

	db := i.connector.(*mysql.Database)

	_, res, err := db.GeneralQuery(i.ctx, fmt.Sprintf(`SELECT
  COALESCE(SUM(sum_latency)/1000000000, 0) AS SUM_LATENCY
FROM information_schema.cluster_statements_summary_history a
WHERE a.summary_begin_time <= NOW()
  AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
  AND a.query_sample_text NOT LIKE '%%/*+ monitoring */%%'`, i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	sumLatency, err := decimal.NewFromString(res[0]["SUM_LATENCY"])
	if err != nil {
		return nil, err
	}

	if sumLatency.Equal(decimal.NewFromInt(0)) {
		return nil, nil
	}

	_, res, err = db.GeneralQuery(i.ctx, fmt.Sprintf(`/*+ monitoring */ SELECT DENSE_RANK() OVER w AS 'plan_counts_rank', aaa.*
FROM (
    SELECT
        COUNT(DISTINCT plan_digest) AS plan_digest_counts,
        ROUND(SUM(sum_latency) / 1000000000, 3) AS total_latency_s,
        SUM(exec_count) AS total_execs,
        CONCAT(ROUND(MIN(min_latency) / 1000000000, 3), ' , ', (
            SELECT plan_digest
            FROM information_schema.cluster_statements_summary_history sub_min
            WHERE sub_min.DIGEST = a.DIGEST
                AND sub_min.sample_user = a.SAMPLE_USER
                AND sub_min.schema_name = a.SCHEMA_NAME 
                AND sub_min.summary_begin_time <= NOW()
                AND sub_min.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
                AND sub_min.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
            ORDER BY min_latency ASC
            LIMIT 1
        )) AS plan_digest_for_min_latency,
        CONCAT(ROUND(MAX(max_latency) / 1000000000, 3), ' , ', (
            SELECT plan_digest
            FROM information_schema.cluster_statements_summary_history sub_max
            WHERE sub_max.DIGEST = a.DIGEST
                AND sub_max.sample_user = a.SAMPLE_USER
                AND sub_max.schema_name = a.SCHEMA_NAME 
                AND sub_max.summary_begin_time <= NOW()
                AND sub_max.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
                AND sub_max.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
            ORDER BY max_latency DESC
            LIMIT 1
        )) AS plan_digest_for_max_latency,
        ROUND(AVG(AVG_TOTAL_KEYS)) AS avg_total_keys,
        ROUND(AVG(AVG_PROCESSED_KEYS)) AS avg_processed_keys,
        ROUND(SUM(sum_latency)/1000000000/%v * 100, 2) AS percentage,
        DIGEST AS sql_digest,
        MIN(QUERY_SAMPLE_TEXT) AS sql_text
    FROM information_schema.cluster_statements_summary_history a
    WHERE a.summary_begin_time <= NOW()
        AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %v MINUTE)
        AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'
    GROUP BY
        SAMPLE_USER,
        DIGEST,
        SCHEMA_NAME
    HAVING COUNT(DISTINCT plan_digest) > 1
) aaa WINDOW w AS (
    ORDER BY plan_digest_counts DESC
) LIMIT 10`, i.inspConfig.WindowMinutes, i.inspConfig.WindowMinutes, sumLatency.String(), i.inspConfig.WindowMinutes))
	if err != nil {
		return nil, err
	}

	var st []*SqlOrderedByPlan
	for _, r := range res {
		st = append(st, &SqlOrderedByPlan{
			SqlPlans:          r["plan_digest_counts"],
			ElapsedTime:       r["total_latency_s"],
			Executions:        r["total_execs"],
			MinSqlPlan:        r["plan_digest_for_min_latency"],
			MaxSqlPlan:        r["plan_digest_for_max_latency"],
			AvgTotalKeys:      r["avg_total_keys"],
			AvgProcessedKeys:  r["avg_processed_keys"],
			SqlTimePercentage: r["percentage"],
			SqlDigest:         r["sql_digest"],
			SqlText:           r["sql_text"],
		})
	}
	return st, nil
}
