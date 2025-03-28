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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/wentaojin/tidba/utils/stringutil"
)

func IsExistedTiUPComponent() (bool, error) {
	cmd := "which tiup"
	command := exec.Command("/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		return false, fmt.Errorf("shell command [%s] exec failed, error: [%v] output: [%v]", cmd, err, stderr.String())
	}
	return true, nil
}

type ClusterList struct {
	Clusters []*Cluster `json:"clusters"`
}

type Cluster struct {
	Name       string `json:"name"`
	User       string `json:"user"`
	Version    string `json:"version"`
	Path       string `json:"path"`
	PrivateKey string `json:"private_key"`
}

func GetDeployedClusterList(clusterName string) (*Cluster, error) {
	cmd := "tiup cluster list --format json"
	command := exec.Command("/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		return nil, fmt.Errorf("shell command [%s] exec failed, error: [%v] output: [%v]", cmd, err, stderr.String())
	}

	var cl *ClusterList
	if err := json.Unmarshal(stdout.Bytes(), &cl); err != nil {
		return nil, err
	}

	for _, c := range cl.Clusters {
		if c.Name == clusterName {
			return c, nil
		}
	}

	return nil, fmt.Errorf("the cluster_name [%v] display not found, please double verify [tiup cluster display %s]", clusterName, clusterName)
}

type ClusterLabel struct {
	ClusterMeta *Meta    `json:"cluster_meta"`
	Labels      []*Label `json:"labels"`
}

type Meta struct {
	ClusterType    string `json:"cluster_type"`
	ClusterName    string `json:"cluster_name"`
	ClusterVersion string `json:"cluster_version"`
	DeployUser     string `json:"deploy_user"`
	SshType        string `json:"ssh_type"`
	TlsEnable      bool   `json:"tls_enable"`
}
type Label struct {
	Machine   string `json:"machine"`
	Port      string `json:"port"`
	Store     uint64 `json:"store"`
	Status    string `json:"status"`
	Leaders   uint64 `json:"leaders"`
	Regions   uint64 `json:"regions"`
	Capacity  string `json:"capacity"`
	Available string `json:"available"`
	Labels    string `json:"labels"`
}

func GetDeployedClusterLabels(clusterName string) (*ClusterLabel, error) {
	cmd := fmt.Sprintf("tiup cluster display %s --labels --format json", clusterName)
	command := exec.Command("/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		return nil, fmt.Errorf("shell command [%s] exec failed, error: [%v] output: [%v]", cmd, err, stderr.String())
	}

	var cl *ClusterLabel
	if err := json.Unmarshal(stdout.Bytes(), &cl); err != nil {
		return nil, err
	}

	return cl, nil
}

func GetDeployedClusterNodeExporterPorrt(clusterPath string) (string, error) {
	cmd := fmt.Sprintf(`grep 'node_exporter_port' %s/meta.yaml | awk -F ":" "{print \$2}"`, clusterPath)
	command := exec.Command("/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		return "", fmt.Errorf("shell command [%s] exec failed, error: [%v] output: [%v]", cmd, err, stderr.String())
	}

	return strings.ReplaceAll(strings.Trim(stdout.String(), "\n"), " ", ""), nil
}

type ClusterTopology struct {
	ClusterMeta *ClusterMeta `json:"cluster_meta"`
	Instances   []*Instance  `json:"instances"`
}

type ClusterMeta struct {
	ClusterType    string   `json:"cluster_type"`
	ClusterName    string   `json:"cluster_name"`
	ClusterVersion string   `json:"cluster_version"`
	DeployUser     string   `json:"deploy_user"`
	SshType        string   `json:"ssh_type"`
	TlsEnable      bool     `json:"tls_enable"`
	DashboardUrl   string   `json:"dashboard_url"`
	GrafanaUrls    []string `json:"grafana_urls"`
}

type Instance struct {
	ID            string `json:"id"`
	Role          string `json:"role"`
	Host          string `json:"host"`
	ManageHost    string `json:"manage_host"`
	Ports         string `json:"ports"`
	OsArch        string `json:"os_arch"`
	Status        string `json:"status"`
	Memory        string `json:"memory"`
	MemoryLimit   string `json:"memory_limit"`
	CpuQuota      string `json:"cpu_quota"`
	Since         string `json:"since"`
	DataDir       string `json:"data_dir"`
	DeployDir     string `json:"deploy_dir"`
	NumaNode      string `json:"numa_node"`
	NumaCores     string `json:"numa_cores"`
	Version       string `json:"version"`
	ComponentName string `json:"ComponentName"`
	Port          uint64 `json:"Port"`
}

func GetDeployedClusterTopology(clusterName string) (*ClusterTopology, error) {
	cmd := fmt.Sprintf(`tiup cluster display %v --format json`, clusterName)

	command := exec.Command("/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		return nil, fmt.Errorf("shell command [%s] exec failed, error: [%v] output: [%v]", cmd, err, stderr.String())
	}
	var cl *ClusterTopology
	if err := json.Unmarshal(stdout.Bytes(), &cl); err != nil {
		return nil, err
	}
	return cl, nil
}

func (topo *ClusterTopology) GetClusterTopologyComponentInstances(componentName string) ([]*Instance, error) {
	var insts []*Instance
	for _, t := range topo.Instances {
		if strings.EqualFold(t.ComponentName, componentName) {
			insts = append(insts, t)
		}
	}
	if len(insts) == 0 {
		return insts, fmt.Errorf("the component_name [%s] is not existed in the cluster_name [%s]", componentName, topo.ClusterMeta.ClusterName)
	}
	return insts, nil
}

func (topo *ClusterTopology) GetClusterTopologyMetadatab() *ClusterMeta {
	return topo.ClusterMeta
}

func (topo *ClusterTopology) GetClusterTopologyHostIps() []string {
	ipUniqs := make(map[string]struct{})
	for _, t := range topo.Instances {
		ipUniqs[t.Host] = struct{}{}
	}
	var ips []string
	for ip, _ := range ipUniqs {
		ips = append(ips, ip)
	}
	return ips
}

func (topo *ClusterTopology) GetClusterMachineComponentInstances() map[string]map[string]int {
	ipUniq := make(map[string]struct{})
	ipComps := make(map[string]map[string]int)
	for _, t := range topo.Instances {
		ipUniq[t.Host] = struct{}{}
	}

	for ip, _ := range ipUniq {
		compCounts := make(map[string]int)
		for _, t := range topo.Instances {
			if ip == t.Host {
				if count, exist := compCounts[strings.ToLower(t.ComponentName)]; exist {
					compCounts[strings.ToLower(t.ComponentName)] = count + 1
				} else {
					compCounts[strings.ToLower(t.ComponentName)] = 1
				}
			}
		}
		ipComps[ip] = compCounts
	}
	return ipComps
}

func (topo *ClusterTopology) GetClusterComponentInstanceNonUpStatus() map[string]map[string]string {
	ipComps := make(map[string]map[string]string)
	for _, t := range topo.Instances {
		if !strings.Contains(strings.ToUpper(t.Status), "UP") {
			cm := make(map[string]string)
			cm[t.ComponentName] = t.Status
			ipComps[t.ID] = cm
		}
	}
	return ipComps
}

func (topo *ClusterTopology) GetClusterTiKVComponentHostDirMountPoints() map[string][]string {
	// Extract the mount point, for example /data/tidb-data/tikv-20166 -> /data
	instDirMountPoints := make(map[string][]string)
	for _, inst := range topo.Instances {
		if inst.ComponentName == ComponentNameTiKV {
			var bs []string
			if inst.DataDir != "-" {
				bs = append(bs, fmt.Sprintf("/%s", strings.Split(inst.DataDir, "/")[1]))
			}
			if inst.DeployDir != "-" {
				bs = append(bs, fmt.Sprintf("/%s", strings.Split(inst.DeployDir, "/")[1]))
			}
			instDirMountPoints[inst.ID] = bs
		}
	}

	// classify data directories by host and remove duplicates
	hostDirMountPoints := make(map[string][]string)
	for inst, v := range instDirMountPoints {
		hostPortSli := strings.Split(inst, ":")
		host := hostPortSli[0]
		if _, ok := hostDirMountPoints[host]; ok {
			hostDirMountPoints[host] = append(hostDirMountPoints[host], v...)
		} else {
			hostDirMountPoints[host] = v
		}
	}
	hostUniDirMountPoints := make(map[string][]string)

	for host, vals := range hostDirMountPoints {
		hostUniDirMountPoints[host] = stringutil.RemoveDeduplicateSlice(vals)
	}
	return hostUniDirMountPoints
}

func (topo *ClusterTopology) GetClusterComponentStatusServicePortMapping() map[string]string {
	ipComps := make(map[string]string)
	for _, t := range topo.Instances {
		portSli := strings.Split(t.Ports, "/")
		var statusPort string
		if len(portSli) == 1 {
			statusPort = portSli[0]
		} else {
			statusPort = portSli[1]
		}
		ipComps[fmt.Sprintf("%s:%s", t.Host, statusPort)] = t.ID
	}
	return ipComps
}

func (topo *ClusterTopology) GetClusterComponentPDComponenetLeaderServiceAddress() string {
	for _, t := range topo.Instances {
		if strings.Contains(strings.ToUpper(t.Status), "L") {
			return t.ID
		}
	}
	return ""
}

func (topo *ClusterTopology) GetClusterComponentHostNodeExporterAddress() []string {
	ipUniqs := make(map[string]struct{})
	for _, t := range topo.Instances {
		ipUniqs[t.Host] = struct{}{}
	}
	var ips []string
	for ip, _ := range ipUniqs {
		ips = append(ips, ip)
	}
	return ips
}

func (topo *ClusterTopology) GetClusterComponentStatusPortByTopSqlCPU() ([]string, []string, string) {
	var (
		tidbs  []string
		tikvs  []string
		ngAddr string
	)
	for _, t := range topo.Instances {
		if t.ComponentName == ComponentNameTiDB {
			portSli := strings.Split(t.Ports, "/")
			var statusPort string
			if len(portSli) == 1 {
				statusPort = portSli[0]
			} else {
				statusPort = portSli[1]
			}
			tidbs = append(tidbs, fmt.Sprintf("%s:%s", t.Host, statusPort))
		} else if t.ComponentName == ComponentNameTiKV {
			tikvs = append(tikvs, t.ID)
		} else if t.ComponentName == ComponentNamePrometheus {
			portSli := strings.Split(t.Ports, "/")
			var ngPort string
			if len(portSli) == 1 {
				ngPort = portSli[0]
			} else {
				ngPort = portSli[1]
			}
			ngAddr = fmt.Sprintf("%s:%s", t.Host, ngPort)
		}

	}
	return tidbs, tikvs, ngAddr
}
