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

func DefaultInspSystemConfigItems() []*SystemConfig {
	return []*SystemConfig{
		{
			CheckItem:     "检查磁盘挂载参数",
			CheckStandard: "TiKV 磁盘的挂载参数含 nodelalloc 和 noatime",
		},
		{
			CheckItem:     "检查主机磁盘平均写延迟是否超过 10ms",
			CheckStandard: "主机磁盘平均写延迟不超过 10ms",
		},
		{
			CheckItem:     "检查主机磁盘平均读延迟是否超过 10ms",
			CheckStandard: "主机磁盘平均读延迟不超过 10ms",
		},
		{
			CheckItem:     "检查 swap 是否关闭",
			CheckStandard: "关闭",
		},
		{
			CheckItem:     "检查透明大页是否关闭",
			CheckStandard: "关闭",
		},
		{
			CheckItem:     "检查部署用户 ID 和组 ID 是否一致",
			CheckStandard: "一致，且所有服务器 ID 相同",
		},
		{
			CheckItem:     "部署用户系统密码是否不过期",
			CheckStandard: "有效期超过 9999 天",
		},
		{
			CheckItem:     "时间同步是否正常",
			CheckStandard: "NTP 或 Chrony 的时间同步正常",
		},
		{
			CheckItem:     "检查 /etc/sysctl.conf 参数",
			CheckStandard: "符合官网最佳实践",
		},
		{
			CheckItem:     "检查 /etc/security/limits.conf 参数",
			CheckStandard: "符合官网最佳实践",
		},
	}
}

func DefaultInspSysctlConfigItems() map[string]int64 {
	return map[string]int64{
		"fs.file-max":             1000000,
		"net.core.somaxconn":      32768,
		"net.ipv4.tcp_tw_recycle": 0,
		"net.ipv4.tcp_syncookies": 0,
		"vm.overcommit_memory":    1,
		"vm.min_free_kbytes":      1048576,
	}
}

func DefaultInspLimitsParamsConfigItems() map[string]int {
	return map[string]int{
		"soft nofile": 1000000,
		"hard nofile": 1000000,
		"soft stack":  32768,
		"hard stack":  32768,
	}
}

func DefaultReportSummaryContent() []*InspectSummary {
	detailText := "信息详情请参阅报告"
	return []*InspectSummary{
		{
			SummaryName:   "3.1 硬件基本信息",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "3.2 软件基本信息",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "3.3 TiDB 集群总览",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.4 开发规范最佳实践",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.5 数据库参数最佳实践",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.6 统计信息最佳实践",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.7 系统配置最佳实践",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.8 crontab 情况",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "3.9 dmesg 情况",
			SummaryResult: "正常",
		},
		{
			SummaryName:   "3.10 数据库的错误日志统计",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "3.11 用户对象占用空间分布",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "4.1 Performance statistics by PD 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "4.2 Performance statistics by TiDB 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "4.3 Performance statistics by TiKV 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "5.1 SQL ordered by Elapsed Time 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "5.2 SQL ordered by TiDB CPU Time 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "5.3 SQL ordered by TiKV CPU Time 检查",
			SummaryResult: detailText,
		},
		{
			SummaryName:   "5.4 SQL ordered by Executions 检查",
			SummaryResult: detailText,
		}, {
			SummaryName:   "5.5 SQL ordered by Plans 检查",
			SummaryResult: detailText,
		},
	}
}
