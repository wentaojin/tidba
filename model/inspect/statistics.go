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
	CheckDesc     string
	CheckItem     string
	CheckStandard string
	CheckSql      string
}

func DefaultInspDatabaseStatisticsItems() []*InspDatabaseStatistics {
	autoInc := NewAutoIncrement(0)
	return []*InspDatabaseStatistics{
		{
			CheckSeq:      autoInc.Next(),
			CheckDesc:     "Inspect whether there are tables with failed statistics collection",
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
			CheckDesc:     "Inspect whether there are tables without histograms",
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
			CheckDesc:     "Inspect whether there are wide tables without setting analyze options",
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
			CheckDesc:     "Inspect whether there are partition tables with unreasonable analyze options",
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
			CheckDesc:     "Inspect whether there are v1/v2 statistics information mixed",
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
			CheckDesc:     "Inspect whether there are invalid tables with statistics information",
			CheckItem:     "是否存在无效表的统计信息",
			CheckStandard: "检查打印已删除的无效表的统计信息",
			CheckSql: `SELECT DISTINCT h.table_id AS SQL_RESULT
FROM mysql.stats_histograms h
WHERE NOT EXISTS (
    SELECT 1 
    FROM information_schema.tables t 
    WHERE t.tidb_table_id = h.table_id
)
AND NOT EXISTS (
    SELECT 1 
    FROM information_schema.partitions p 
    WHERE p.TIDB_PARTITION_ID = h.table_id AND p.tidb_partition_id IS NOT NULL
)`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckDesc:     "Inspect whether there are normal tables without statistics information",
			CheckItem:     "是否存在普通表缺失统计信息",
			CheckStandard: "检查是否存在普通表缺失统计信息",
			CheckSql: `WITH tb AS (
  SELECT
    table_schema,
    table_name,
    tidb_table_id
  FROM information_schema.tables
  WHERE table_schema NOT IN ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA')
    AND table_type IN ('BASE TABLE','SEQUENCE') 
    AND COALESCE(create_options,'') NOT LIKE '%partitioned%'
),
st AS (
  SELECT
    table_id,
    MAX(stats_ver) AS max_stats_ver
  FROM mysql.stats_histograms
  GROUP BY table_id
)
SELECT
  CONCAT(tb.table_schema, '.', tb.table_name) AS SQL_RESULT
FROM tb
LEFT JOIN st ON tb.tidb_table_id = st.table_id
WHERE COALESCE(st.max_stats_ver, 0) = 0`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckDesc:     "Inspect whether there are partition tables without statistics information",
			CheckItem:     "是否存在分区缺失 partition 统计信息",
			CheckStandard: "检查是否存在分区缺失 partition 统计信息",
			CheckSql: `WITH pt AS (
  SELECT
    p.table_schema,
    p.table_name,
    p.partition_name,
    p.tidb_partition_id
  FROM information_schema.partitions p
  WHERE p.partition_name IS NOT NULL
    AND p.table_schema NOT IN ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA')
),
st AS (
  SELECT
    table_id,
    MAX(COALESCE(stats_ver, 0)) AS max_stats_ver
  FROM mysql.stats_histograms
  GROUP BY table_id
)
SELECT
  CONCAT(pt.table_schema, '.', pt.table_name, '.', pt.partition_name) AS SQL_RESULT
FROM pt
LEFT JOIN st
  ON pt.tidb_partition_id = st.table_id
WHERE COALESCE(st.max_stats_ver, 0) = 0`,
		},
		{
			CheckSeq:      autoInc.Next(),
			CheckDesc:     "Inspect whether there are partition tables without statistics information",
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
			CheckDesc:     "Inspect whether there are locked statistics information tables",
			CheckItem:     "是否存在被锁定统计信息的表",
			CheckStandard: "v6.5 的锁定统计信息不建议使用，建议 v8.1 版本以上使用",
			CheckSql: `SELECT CONCAT(t.table_schema,'.',t.table_name) AS SQL_RESULT
FROM mysql.stats_table_locked l 
INNER JOIN information_schema.tables t 
ON l.table_id = t.tidb_table_id`,
		},
	}
}
