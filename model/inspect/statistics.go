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

type InspDatabaseStatisticsAbnormalOutput struct {
	*InspDatabaseStatistics
	AbnormalDetail string
	AbnormalCounts int
	Comment        string
}

type InspDatabaseStatistics struct {
	CheckSeq      int
	CheckItem     string
	CheckStandard string
	CheckSql      string
}

func DefaultInspDatabaseStatisticsItems() []*InspDatabaseStatistics {
	autoInc := NewAutoIncrement(0)
	return []*InspDatabaseStatistics{
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在统计信息收集失败的表",
			CheckStandard: "检查打印出最近 5 个统计信息收集失败的表",
			CheckSql: `SELECT CONCAT(table_schema, '.', table_name) AS SQL_RESULT FROM 
				information_schema.analyze_status 
			WHERE (table_schema, table_name) in (
	select table_schema, table_name from INFORMATION_SCHEMA.analyze_status 
	where state != 'finished' 
	order by start_time desc 
	limit 5)`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在健康度小于 90% 的表",
			CheckStandard: "检查打印出健康度小于 90% 的表",
			CheckSql: `SELECT CONCAT(t.table_schema, '.', t.table_name) AS SQL_RESULT
FROM information_schema.tables t 
INNER JOIN (
	SELECT table_id, IF(modify_count >= count, 0, (1 - modify_count / count) * 100) AS healthy, modify_count, count 
	FROM mysql.stats_meta 
	WHERE count != 0 
	HAVING healthy < 90 
	UNION ALL 
	SELECT table_id, 0, modify_count, count 
	FROM mysql.stats_meta 
	WHERE count = 0 AND modify_count != 0
) x ON t.tidb_table_id = x.table_id`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在没有直方图的表",
			CheckStandard: "检查打印出没有直方图的表",
			CheckSql: `SELECT CONCAT(table_schema, '.', table_name) AS SQL_RESULT
FROM information_schema.tables 
WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'test', 'mysql', 'sys') 
AND tidb_table_id NOT IN (
	SELECT DISTINCT table_id 
	FROM mysql.stats_histograms
)`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在宽表没有设置收集策略",
			CheckStandard: "宽表(columns > 200)，建议设置正确的收集策略",
			CheckSql: `SELECT CONCAT(c.table_schema, '.', c.table_name) AS SQL_RESULT
FROM (
    SELECT table_schema, table_name, COUNT(*) AS cnt
    FROM information_schema.columns
    WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'test', 'mysql', 'sys')
    GROUP BY table_schema, table_name
    HAVING cnt > 200
) c
INNER JOIN information_schema.tables t 
    ON c.table_schema = t.table_schema AND c.table_name = t.table_name
LEFT JOIN mysql.analyze_options o 
    ON t.tidb_table_id = o.table_id
WHERE o.column_choice IS NULL OR o.column_choice IN ('DEFAULT', 'ALL')`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在分区表统计收集策略不合理",
			CheckStandard: "在动态分区裁剪模式，分区表(超过 30 个分区)，建议设置正确的收集策略",
			CheckSql: `SELECT CONCAT(p.table_schema, '.', p.table_name) AS SQL_RESULT 
FROM (
	SELECT table_schema, table_name, COUNT(*) AS cnt 
	FROM information_schema.partitions 
	WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'test', 'mysql', 'sys') 
	GROUP BY table_schema, table_name 
	HAVING cnt > 30
) p 
INNER JOIN information_schema.tables t ON p.table_schema = t.table_schema AND p.table_name = t.table_name 
LEFT JOIN mysql.analyze_options o ON t.tidb_table_id = o.table_id 
WHERE o.column_choice IS NULL OR o.column_choice IN ('DEFAULT', 'ALL')`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否 v1/v2 统计信息并存",
			CheckStandard: "判断系统 v1/v2 统计信息混用",
			CheckSql: `SELECT '存在混用' AS SQL_RESULT
FROM mysql.stats_histograms 
WHERE stats_ver = 1 
AND EXISTS (
	SELECT 1 
	FROM mysql.stats_histograms 
	WHERE stats_ver = 2
) 
LIMIT 1`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在无效表的统计信息",
			CheckStandard: "检查打印已删除的无效表的统计信息",
			CheckSql: `SELECT DISTINCT table_id AS SQL_RESULT
FROM mysql.stats_histograms h 
WHERE h.table_id NOT IN (
	SELECT t.tidb_table_id 
	FROM information_schema.tables t 
	UNION ALL 
	SELECT p.TIDB_PARTITION_ID 
	FROM information_schema.partitions p
)`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在普通表缺失统计信息",
			CheckStandard: "检查是否存在普通表缺失统计信息",
			CheckSql: `SELECT CONCAT(info2.tb_schema, '.', info2.tb_name) AS SQL_RESULT
FROM (
	SELECT info.tb_schema, info.tb_name, MAX(DATE_FORMAT(tidb_parse_tso(version), '%Y-%m-%d %H:%i:%s')) AS analyze_time, SUM(stats_ver) AS stats_version, COUNT(1) AS hist_count 
	FROM (
		SELECT tb.TABLE_SCHEMA AS tb_schema, tb.TABLE_NAME AS tb_name, tb.tidb_table_id, sm.table_id, sm.hist_id, sm.version, sm.stats_ver 
		FROM information_schema.tables tb 
		LEFT JOIN mysql.stats_histograms sm 
		ON tb.tidb_table_id = sm.table_id 
		WHERE tb.TABLE_TYPE = 'BASE TABLE' 
		AND tb.CREATE_OPTIONS <> 'partitioned' 
		AND tb.TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA', 'METRICS_SCHEMA')
	) info 
	GROUP BY info.tb_schema, info.tb_name
) info2 
WHERE stats_version = 0 OR stats_version IS NULL 
ORDER BY info2.tb_schema, info2.tb_name`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在分区缺失 partition 统计信息",
			CheckStandard: "检查是否存在分区缺失 partition 统计信息",
			CheckSql: `SELECT CONCAT(info3.tb_schema, '.', info3.tb_name, '.', info3.partition_name) AS SQL_RESULT 
FROM (
	SELECT info2.tb_schema, info2.tb_name, info2.partition_name, CASE 
		WHEN stats_version = 0 OR stats_version IS NULL THEN 'Miss Stats' 
		WHEN stats_version > 0 THEN 'Normal stats' ELSE 'Error Stats' 
	END AS Stats_info, CASE 
		WHEN stats_version > 0 THEN STR_TO_DATE(analyze_time, '%Y-%m-%d %H:%i:%s') 
		WHEN stats_version = 0 OR stats_version IS NULL THEN 'No Analyze Time' 
		ELSE 'No Analyze Time' 
	END AS Analyze_Time 
	FROM (
		SELECT info.tb_schema, info.tb_name, info.partition_name, MAX(DATE_FORMAT(tidb_parse_tso(version), '%Y-%m-%d %H:%i:%s')) AS analyze_time, SUM(stats_ver) AS stats_version, COUNT(1) AS hist_count 
		FROM (
			SELECT tb.TABLE_SCHEMA AS tb_schema, tb.TABLE_NAME AS tb_name, tbp.partition_name, tb.tidb_table_id, tbp.TIDB_PARTITION_ID, sm.table_id, sm.hist_id, sm.version, sm.stats_ver 
			FROM information_schema.tables tb 
			LEFT JOIN information_schema.partitions tbp 
			ON tb.table_schema = tbp.table_schema AND tb.table_name = tbp.table_name 
			LEFT JOIN mysql.stats_histograms sm 
			ON tbp.TIDB_PARTITION_ID = sm.table_id 
			WHERE tbp.partition_name IS NOT NULL 
			AND tb.TABLE_TYPE = 'BASE TABLE' 
			AND tb.CREATE_OPTIONS = 'partitioned'
		) info 
		GROUP BY info.tb_schema, info.tb_name, partition_name
	) info2
) info3 
WHERE info3.Stats_info <> 'Normal stats' 
ORDER BY info3.tb_schema, info3.tb_name, info3.partition_name`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在分区表缺失 global 统计信息",
			CheckStandard: "检查是否存在分区表缺失 global 统计信息",
			CheckSql: `SELECT CONCAT(info2.tb_schema, '.', info2.tb_name) AS SQL_RESULT
FROM (
	SELECT info.tb_schema, info.tb_name, SUM(stats_ver) AS stats_version 
	FROM (
		SELECT tb.TABLE_SCHEMA AS tb_schema, tb.TABLE_NAME AS tb_name, tb.tidb_table_id, sm.table_id, sm.hist_id, sm.version, sm.stats_ver 
		FROM information_schema.tables tb 
		LEFT JOIN mysql.stats_histograms sm 
		ON tb.tidb_table_id = sm.table_id 
		WHERE tb.TABLE_TYPE = 'BASE TABLE' 
		AND tb.CREATE_OPTIONS = 'partitioned' 
		AND tb.TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA', 'METRICS_SCHEMA')
	) info 
	GROUP BY info.tb_schema, info.tb_name
) info2 
WHERE info2.stats_version = 0 OR info2.stats_version IS NULL 
ORDER BY info2.tb_schema, info2.tb_name`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckItem:     "是否存在被锁定统计信息的表",
			CheckStandard: "v6.5 的锁定统计信息不建议使用，建议 v8.1 版本以上使用",
			CheckSql: `SELECT CONCAT(t.table_schema,'.',t.table_name) AS SQL_RESULT
FROM mysql.stats_table_locked l 
INNER JOIN information_schema.tables t 
ON l.table_id = t.tidb_table_id`,
		},
	}
}
