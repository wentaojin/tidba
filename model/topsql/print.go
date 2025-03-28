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

import "fmt"

func PrintTopsqlElapsedTimeComment(top int) {
	fmt.Printf("记录时间窗口内 SQL 执行总耗时排序 TOP %d，按照从大到小的顺序排列\n", top)
	fmt.Println(`
Elapsed Time(s)：SQL 执行总耗时
Executions：SQL 执行总次数
Elap per Exec(s)：每次执行平均耗时
Min query Time(s)：SQL 最小执行耗时
Max query Time(s)：SQL 最大执行耗时
Avg total keys：Coprocessor 扫过的 key 的平均数量
Avg processed keys：Coprocessor 处理的 key 的平均数量。相比 avg_total_keys，avg_processed_keys 不包含 MVCC 的旧版本。如果 avg_total_keys 和 avg_processed_keys 相差很大，说明旧版本比较多
% Total SQL Time：巡检时间占 SQL 总体耗时占比
SQL Digest：SQL 指纹
SQL Text：SQL 文本`)
}

func PrintTopsqlExecutionsComment(top int) {
	fmt.Printf("记录时间窗口内 SQL 执行次数排序 TOP %d，按照从大到小的顺序排列\n", top)
	fmt.Println("OLTP 系统的话，这部分比较有用。SQL执行频率非常大，SQL的执行次数会对性能有比较大的影响")
	fmt.Println("OLAP 系统 SQL重复执行的频率很低，因此意义不大")
	fmt.Println(`
Executions：SQL 执行总次数
Elap per Exec(s)：每次执行平均 SQL 耗时
Parse per Exec(s)：每次执行平均解析耗时
Compile per Exec(s)：每次执行平均编译耗时
Min query Time(s)：SQL 最小执行耗时
Max query Time(s)：SQL 最大执行耗时
Avg total keys：Coprocessor 扫过的 key 的平均数量
Avg processed keys：Coprocessor 处理的 key 的平均数量。相比 avg_total_keys，avg_processed_keys 不包含 MVCC 的旧版本。如果 avg_total_keys 和 avg_processed_keys 相差很大，说明旧版本比较多
% Total SQL Time：巡检时间占 SQL 总体耗时占比
SQL Digest：SQL 指纹
SQL Text：SQL 文本`)
}

func PrintTopsqlPlansComment(top int) {
	fmt.Printf("记录时间窗口内 SQL 执行计划变化 TOP %d，按照执行计划变化次数排序排列\n", top)
	fmt.Println("NOTES：执行计划超过 1 次的才会被显示")
	fmt.Println(`
SQL plans：SQL 执行计划变化次数
Elapsed Time(s)：SQL 执行总耗时
Executions：SQL 执行总次数
Min sql Plan(s)：耗时最小的执行计划耗时（plan elapsed  , plan digest）
Max sql Plan(s): 耗时最大的执行计划耗时（plan elapsed  , plan digest）
Avg total keys：Coprocessor 扫过的 key 的平均数量
Avg processed keys：Coprocessor 处理的 key 的平均数量。相比 avg_total_keys，avg_processed_keys 不包含 MVCC 的旧版本。如果 avg_total_keys 和 avg_processed_keys 相差很大，说明旧版本比较多
% Total SQL Time：巡检时间占 SQL 总体耗时占比
SQL Digest：SQL 指纹
SQL Text：SQL 文本`)
}

func PrintTopsqlCpuByTidbComment(top int) {
	fmt.Printf("记录时间窗口内执行 SQL 指纹占 TiDB 组件 OR 实例维度 CPU 时间总和时间 TOP %d\n", top)
	fmt.Println("NOTES：请注意是监控范围内该 SQL 的执行占 TiDB CPU 时间总和，而不是单次 SQL 执行时间")
	fmt.Println(`
CPU Time(s)：SQL 执行 CPU 总消耗
Exec counts per sec：每秒执行 SQL 执行次数
Latency per exec：每次执行平均 SQL 耗时
Scan record per sec：每秒扫描表记录数
Scan Indexes per sec：每秒扫描索引记录数
Plan digest counts：产生执行计划数
Max plan sql latency：执行计划最差的耗时
Min plan sql latency：执行计划最好的耗时
% Total SQL Time：SQL 耗时时间占时间窗口内 SQL 总体耗时占比
SQL Digest：SQL 指纹
SQL Text：SQL 文本（参数化）`)
}

func PrintTopsqlCpuByTikvComment(top int) {
	fmt.Printf("记录时间窗口内执行 SQL 指纹占 TiKV 组件 OR 实例维度 CPU 时间总和时间 TOP %d\n", top)
	fmt.Println("NOTES：请注意是监控范围内该 SQL 的执行占 TiKV CPU 时间总和，而不是单次 SQL 执行时间")
	fmt.Println(`
CPU Time(s)：SQL 执行 CPU 总消耗
Exec counts per sec：每秒执行 SQL 执行次数
Latency per exec：每次执行平均 SQL 耗时
Scan record per sec：每秒扫描表记录数
Scan Indexes per sec：每秒扫描索引记录数
Plan digest counts：产生执行计划数
Max plan sql latency：执行计划最差的耗时
Min plan sql latency：执行计划最好的耗时
% Total SQL Time：SQL 耗时时间占时间窗口内 SQL 总体耗时占比
SQL Digest：SQL 指纹
SQL Text：SQL 文本（参数化）`)
}
