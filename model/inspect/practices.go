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

type InspDevBestPracticesAbnormalOutput struct {
	*InspDevBestPractices
	AbnormalDetail string
	AbnormalCounts int
}

type InspDevBestPractices struct {
	CheckSeq          int
	CheckItem         string
	CheckCategory     string
	RectificationType string
	CheckType         string
	BestPracticeDesc  string
	CheckSql          string
}

func DefaultDevBestPracticesInspItems() []*InspDevBestPractices {
	autoInc := NewAutoIncrement(0)

	return []*InspDevBestPractices{
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "无主键或唯一键",
			CheckCategory:     "建表规范",
			RectificationType: "强烈建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "表需要有主键或唯一键",
			CheckSql: `SELECT
		concat(table_schema, '.' , table_name) AS SQL_RESULT
	FROM
		INFORMATION_SCHEMA.TABLES
	WHERE
		(table_name,table_schema) NOT IN (
		SELECT
			table_name,
			table_schema
		FROM
			INFORMATION_SCHEMA.tidb_indexes
		WHERE
			NON_UNIQUE = 0 )
		AND 1 = 1
		AND TABLE_SCHEMA NOT IN ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "大表使用主键自增属性",
			CheckCategory:     "建表规范",
			RectificationType: "强烈建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "写入量较大的表，应避免使用连续自增的值对主键进行填充",
			CheckSql: `SELECT
		concat(c.table_schema, '.' , c.table_name) AS SQL_RESULT
	FROM
		(
		SELECT
			table_schema,
			table_name
		FROM
			INFORMATION_SCHEMA.COLUMNS
		WHERE
			extra = 'auto_increment'
			AND 1 = 1
			AND TABLE_SCHEMA NOT IN ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA') ) c
	JOIN INFORMATION_SCHEMA.tables t ON
		t.TABLE_SCHEMA = c.TABLE_SCHEMA
		AND t.table_name = c.TABLE_NAME
	WHERE
		t.table_rows > 10000000`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用外键",
			CheckCategory:     "建表规范",
			CheckType:         "表",
			RectificationType: "强烈建议整改",
			BestPracticeDesc:  "TiDB 仅部分支持外键约束功能，不建议使用",
			CheckSql: `SELECT
		concat(table_schema, '.' , table_name) AS SQL_RESULT
	FROM
		INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	WHERE
		1 = 1
		AND TABLE_SCHEMA NOT IN ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA')
		AND REFERENCED_TABLE_NAME IS NOT NULL`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用longblob",
			CheckCategory:     "数据类型",
			CheckType:         "字段",
			RectificationType: "强烈建议整改",
			BestPracticeDesc:  "longblob 类型最大列长度为4G。但由于 TiDB 单列的限制，TiDB 中默认单列存储最大不超过 6 MiB，可通过配置项将该限制调整至 120 MiB",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type='longblob'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用longtext",
			CheckCategory:     "数据类型",
			RectificationType: "强烈建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "longtext最大列长度为4G。但由于 TiDB 单列的限制，TiDB 中默认单列存储最大不超过 6 MiB，可通过配置项将该限制调整至 120 MiB",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type='longtext'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用TIMESTAMP",
			CheckCategory:     "数据类型",
			RectificationType: "强烈建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "禁止使用 TIMESTAMP类型（TIMESTAMP 数据类型受 2038 年问题的影响）",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type='timestamp'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "存储精度浮点数使用 float 或 double",
			CheckCategory:     "数据类型",
			RectificationType: "强烈建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "浮点类型推荐使用 DECIMAL 类型，float 和 double 在存储的时候，存在精度损失的问题，很可能在值的比较时，得到不正确的结果，不建议使用",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type in ('double','float')
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表名使用中文",
			CheckCategory:     "命名规范",
			RectificationType: "强烈建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "表命名只能使用英文字母、数字、下划线",
			CheckSql: `select concat( table_schema, '.', table_name ) AS SQL_RESULT
					   from information_schema.tables
					   where 1 = 1
					   and table_name not REGEXP'[A-z]'
					   and table_name not REGEXP'[0-9]'
					   and TABLE_SCHEMA NOT IN ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表字段名使用中文",
			CheckCategory:     "命名规范",
			RectificationType: "强烈建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "字段命名只能使用英文字母、数字、下划线",
			CheckSql: `select concat( table_schema, '.', table_name, '.', column_name ) AS SQL_RESULT
					   from information_schema.columns
					   where 1 = 1
					   and column_name not REGEXP'[A-z]'
					   and column_name not REGEXP'[0-9]'
					   and TABLE_SCHEMA NOT IN ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "主键类型不为bigint",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "尽量不选择字符串列作为主键",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where data_type <>'bigint'
					   and column_key='PRI'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表级别字符集检查",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "表级别字符集建议使用 utf8mb4 或 gbk",
			CheckSql: `select concat( table_schema, '.' , table_name ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.TABLES
					   where TABLE_COLLATION not in ('utf8mb4_bin','gbk_chinese_ci')
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "字段级别字符集检查",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "字段级别字符集建议 utf8mb4 或 gbk，字符序为 utf8mb4_bin 或 gbk_chinese_ci",
			CheckSql: `select concat( table_schema, '.', table_name, '.', COLUMN_NAME, ' CHARACTER_SET_NAME is ', CHARACTER_SET_NAME, ' COLLATION_NAME is ', COLLATION_NAME )  AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where ( CHARACTER_SET_NAME not in ('utf8mb4','gbk') OR COLLATION_NAME not in ('utf8mb4_bin','gbk_chinese_ci') )
					   and 1 = 1
					   and TABLE_SCHEMA NOT IN ( 'mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA' )`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "设置 NOT NULL 无默认值",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "字段设置了NOT NULL需设置默认值",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where is_nullable='NO'
					   and COLUMN_DEFAULT is NULL
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表无注释",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "表需要有注释",
			CheckSql: `select concat( table_schema, '.' , table_name ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.tables
					   where table_comment=''
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "字段无注释",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "字段需要有注释",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where (COLUMN_COMMENT is null or COLUMN_COMMENT = '')
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表字段数超过 80 个",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "出于为性能考虑，尽量避免存储超宽表，表字段数不建议超过 80 个",
			CheckSql: `select concat( table_schema, '.', table_name ) AS SQL_RESULT
					   from ( SELECT table_schema,table_name
							  from information_schema.columns
							  where 1 = 1
							  and TABLE_SCHEMA NOT IN ( 'mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA' )
							  group by table_schema,table_name
							  having count(1) > 80 ) t;`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表平均单行数据超过 64k",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "表",
			BestPracticeDesc:  "出于为性能考虑，尽量避免存储超宽表，建议单行的总数据大小不要超过 64K",
			CheckSql: `select concat( table_schema, '.', table_name ) AS SQL_RESULT
					   from information_schema.tables
					   where 1 = 1
					   AND AVG_ROW_LENGTH > 65536
					   AND TABLE_SCHEMA NOT IN ( 'mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA' )`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "存在冗余索引",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "索引",
			BestPracticeDesc:  "避免冗余索引",
			CheckSql: `SELECT
	concat( a.table_schema, '.' , a.table_name, '索引' , a.index_name, '和', b.index_name, '冗余' )
FROM
	( (
	SELECT
		information_schema.STATISTICS.TABLE_SCHEMA AS table_schema ,
		information_schema.STATISTICS.TABLE_NAME AS table_name ,
		information_schema.STATISTICS.INDEX_NAME AS index_name ,
		max( information_schema.STATISTICS.NON_UNIQUE ) AS non_unique ,
		max( IF ( isnull( information_schema.STATISTICS.SUB_PART ), 0, 1 ) ) AS subpart_exists ,
		group_concat( information_schema.STATISTICS.COLUMN_NAME
	ORDER BY
		information_schema.STATISTICS.SEQ_IN_INDEX ASC SEPARATOR ',' ) AS index_columns
	FROM
		information_schema.STATISTICS
	WHERE
		( ( information_schema.STATISTICS.INDEX_TYPE = 'BTREE' )
			AND 1 = 1 )
	GROUP BY
		information_schema.STATISTICS.TABLE_SCHEMA ,
		information_schema.STATISTICS.TABLE_NAME ,
		information_schema.STATISTICS.INDEX_NAME ) a
JOIN (
	SELECT
		information_schema.STATISTICS.TABLE_SCHEMA AS table_schema ,
		information_schema.STATISTICS.TABLE_NAME AS table_name ,
		information_schema.STATISTICS.INDEX_NAME AS index_name ,
		max( information_schema.STATISTICS.NON_UNIQUE ) AS non_unique ,
		max( IF ( isnull( information_schema.STATISTICS.SUB_PART ), 0, 1 ) ) AS subpart_exists ,
		group_concat( information_schema.STATISTICS.COLUMN_NAME
	ORDER BY
		information_schema.STATISTICS.SEQ_IN_INDEX ASC SEPARATOR ',' ) AS index_columns
	FROM
		information_schema.STATISTICS
	WHERE
		( ( information_schema.STATISTICS.INDEX_TYPE = 'BTREE' )
			AND ( information_schema.STATISTICS.TABLE_SCHEMA NOT IN ( 'mysql', 'sys', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA' ) ) )
	GROUP BY
		information_schema.STATISTICS.TABLE_SCHEMA ,
		information_schema.STATISTICS.TABLE_NAME ,
		information_schema.STATISTICS.INDEX_NAME ) b ON
	( ( ( a. table_schema = b. table_schema )
		AND ( a. table_name = b. table_name ) ) ) )
WHERE
	( ( a. index_name <> b. index_name )
		AND ( ( ( a. index_columns = b. index_columns )
			AND ( ( a. non_unique > b. non_unique )
				OR ( ( a. non_unique = b. non_unique )
					AND ( IF ( (a. index_name = 'PRIMARY'),
					'',
					a. index_name ) > IF ( (b. index_name = 'PRIMARY'),
					'',
					b. index_name ) ) ) ) )
			OR ( ( locate( concat(a. index_columns , ','),
			b. index_columns ) = 1 )
				AND (a. non_unique = 1) )
				OR ( ( locate( concat(b. index_columns , ','),
				a. index_columns ) = 1 )
					AND (b. non_unique = 0) ) ) )`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表索引个数超 5 个",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "索引",
			BestPracticeDesc:  "单张表的索引数量控制在 5 个以内",
			CheckSql: `select concat( table_schema, '.' , table_name ) AS SQL_RESULT
					   from ( select a.table_schema, a.table_name, count(*)
							  from ( select distinct table_schema,table_name,key_name
									 from information_schema.tidb_indexes
									 where 1 = 1 ) a
							  group by a.table_schema,a.table_name
							  having count(*)>5 ) t;`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表索引字段个数超 5 个",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "索引",
			BestPracticeDesc:  "索引中的字段数建议不超过 5 个",
			CheckSql: `select concat( table_schema, '.', table_name, '.', KEY_NAME )  AS SQL_RESULT
					   from ( select distinct table_schema, table_name, KEY_NAME
							  from information_schema.tidb_indexes
							  where 1 = 1 AND TABLE_SCHEMA NOT IN ( 'mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA' )
							  group by TABLE_SCHEMA,table_name,KEY_NAME
							  having count(1) > 1 ) t;`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "唯一索引存在字段可为空",
			CheckCategory:     "建表规范",
			RectificationType: "建议整改",
			CheckType:         "索引",
			BestPracticeDesc:  "表需要有主键或者唯一索引，需要唯一索引所有字段非空（避免出现多条空值的重复记录）",
			CheckSql: `select concat( c.table_schema, '.', c.table_name, '.', c.COLUMN_NAME ) AS SQL_RESULT
					   from ( select table_schema, table_name, column_name, is_nullable
							  from information_schema.columns
							  where 1 = 1 AND TABLE_SCHEMA NOT IN ( 'mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA' )
							  and is_nullable = 'YES' ) c
					   join Information_Schema.Key_Column_Usage k
					   on c.TABLE_SCHEMA = k.TABLE_SCHEMA
					   and c.TABLE_NAME = k.TABLE_NAME
					   and c.COLUMN_NAME = k.COLUMN_NAME;`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用 blob 或 text",
			CheckCategory:     "数据类型",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "不推荐使用复杂的数据类型 TEXT 和 BLOB",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where data_type in ( 'blob','text' )
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用 mediumtext",
			CheckCategory:     "数据类型",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "mediutext 最大支持 16M，TiDB 限制了单条 KV entry 不超过 6MiB。可以修改配置文件中的 txn-entry-size-limit 配置项进行调整，最大可以修改到 120MiB",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type='mediumtext'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用 enum、set 类型",
			CheckCategory:     "数据类型",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "不建议使用 ENUM、SET 类型，尽量使用 TINYINT 来代替",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where column_type in ('enum','set')
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "表名存在大写",
			CheckCategory:     "命名规范",
			RectificationType: "提示",
			CheckType:         "表",
			BestPracticeDesc:  "表名建议小写",
			CheckSql: `select concat( table_schema, '.' , table_name ) AS SQL_RESULT
					   from information_schema.tables
					   where 1 = 1
					   and CAST(table_NAME AS BINARY) RLIKE '[A-Z]'
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "字段名存在大写",
			CheckCategory:     "命名规范",
			RectificationType: "提示",
			CheckType:         "字段",
			BestPracticeDesc:  "字段名建议小写",
			CheckSql: `SELECT
		concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
	FROM
		information_schema.columns
	WHERE
		1 = 1
		AND CAST(COLUMN_NAME AS BINARY) RLIKE '[A-Z]'
		AND TABLE_SCHEMA NOT IN ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA', 'METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用 JSON 类型",
			CheckCategory:     "数据类型",
			RectificationType: "建议整改",
			CheckType:         "字段",
			BestPracticeDesc:  "JSON 在 TiDB v6.5 之前为实验特性，不建议生产环境使用",
			CheckSql: `select concat( table_schema, '.' , table_name, '.' , COLUMN_NAME ) AS SQL_RESULT
					   from INFORMATION_SCHEMA.COLUMNS
					   where data_type ='json'
					   and 1 = 1
					   and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA')`,
		},
		{
			CheckSeq:          autoInc.Next(),
			CheckItem:         "使用分区",
			CheckCategory:     "建表规范",
			RectificationType: "提示",
			CheckType:         "表",
			BestPracticeDesc:  "分区表功能与运维特性在 v6.5 之后逐渐 GA 与完善，尽量保证当前版本 >= v6.5",
			CheckSql: `select concat( table_schema, '.' , table_name) AS SQL_RESULT
					   from ( select distinct table_schema, table_name
							  from information_schema.PARTITIONS
							  where partition_name is not null
							  and 1 = 1
							  and TABLE_SCHEMA not in ('mysql','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','METRICS_SCHEMA') ) t;`,
		},
	}
}
