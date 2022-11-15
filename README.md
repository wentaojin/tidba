# TiDBA

TiDB 数据库 DBA 常用工具集，已包含功能：

diff 功能集：
版本升级，集群组件配置文件以及 tidb 专有系统变量 Diff 对比，现阶段只支持 3.0 diff 4.0 或者 4.0 (及以上版本) diff 4.0 (及以上版本)
- v4.0 以下 tikv 需要单独从 tikv 日志中获取配置文件 --base-json 用于对比
- v4.0 及以上可不需要，直接对比 --base-addr 、--compare-addr

diff tidb
```
示例：
-- 终端带颜色输出
$ ./tidba diff tidb --base-addr  {host}:{service-port} --base-user {user}  --base-pass {password} --compare-addr  {host}:{service-port}  --compare-user {user}  --compare-pass {password} --diff-type {config/variable} --coloring --quiet

-- 纯文本输出
$ ./tidba diff tidb --base-addr  {host}:{service-port} --base-user {user}  --base-pass {password} --compare-addr  {host}:{service-port}  --compare-user {user}  --compare-pass {password} --diff-type {config/variable} --quiet >> diff_tidb.log
```
diff pd
```
示例：
-- 终端带颜色输出
$ ./tidba diff pd --base-addr {host}:{service-port} --compare-addr {host}:{service-port} --coloring --quiet

-- 纯文本输出
$ ./tidba diff pd --base-addr {host}:{service-port} --compare-addr {host}:{service-port} --quiet  >> diff_pd.log
```
diff tikv
```
v3.0 json 获取 tikv 日志配置（只有 v3.0 版本需要）,将 config 等于号后面的双引号中的内容全部复制倒另外一个文件内（注意不能带有双引号复制），用于 diff 对比
[2020/12/05 14:29:53.717 +08:00] [INFO] [server.rs:51] ["using config"] [config="{\"log-level\":\"info\",\"log-file\":\"/data1/nightly/tikv-41260/log/tikv.log\",....}"]

示例：
-- 终端带颜色输出
$ ./tidba diff tikv --base-json {json 文件名} --compare-addr {host}:{status-port} --coloring --quiet
$ ./tidba diff tikv --base-addr {host}:{status-port} --compare-addr {host}:{status-port} --coloring --quiet

-- 纯文本输出
$ ./tidba diff tikv --base-json {json 文件名} --compare-addr {host}:{status-port} --quiet >> diff_tikv.log
$ ./tidba diff tikv --base-addr {host}:{status-port} --compare-addr {host}:{status-port} --quiet >> diff_tikv.log
```
split 功能集：[每种打散方式资源消耗以及场景可能情况不同，子命令分为基于实际数据表打散（key、range）、基于采样基础表打散(estimate、sampling、reckon)]
- 基于 region range 生成打散语句
- 基于 region key 生成打散语句
- 基于 region estimate 数据估算生成打散语句
- 基于 region sampling 生成打散语句
- 基于 region reckon 生成打散语句
```
示例：
-- 基于已有数据，根据 range 生成打散
$ ./tidba split range --host {host} --port {port} --user {user} --password {password} --db {db_name} -i {table1,table2} --out-dir {outDir}

-- 基于已有数据，根据 key 生成打散
$ ./tidba split key --host {host} --port {port} --user {user} --password {password} --status-port {status-port} --db {db_name} -i {table1,table2} --out-dir {outDir}

-- 基于基础采样表，根据 distinct 生成打散（因子放大倍数，数据量大的情况下资源耗用主要在于机器资源且比较大）
$ ./tidba split estimate --host {host} --port {port} --user {user} --password {password} --db {db_name} -i {table} --estimate-column {index_column} --gen-db {sql_db} --gen-table {sql_table} --gen-index {sql_index_name} --estimate-row {sql_table_estimate_rows} --estimate-size {sql_table_estimate_size_M}  --out-dir {outDir}

-- 基于基础采样表，根据 distinct 生成打散（数据量大的情况下,资源耗用主要在于机器资源且相对折中）
$ ./tidba split sampling --host {host} --port {port} --user {user} --password {password} --base-db {db_name} --base-table {base-table} --base-index {base-index-name}  --estimate-row {estimate-table-rows} --gen-db {estimate-db} --gen-table {estimate-table-name}  --gen-index {estimate-index-name} --out-dir {outDir}

-- 基于基础采样表，根据 distinct 生成打散（数据量大的情况下,资源耗用主要位于数据库）
$./tidba split reckon --host {host} --port {port} --user {user} --password {password} --base-db {db_name} --base-table {base-table} --base-index {base-index-name} --estimate-row {estimate-table-rows} --gen-db {estimate-db} --gen-table {estimate-table-name}  --gen-index {estimate-index-name} --out-dir {outDir}

```
cluster 功能集：
- 查找集群内只有指定副本数的所有 region 信息
- 当 down store 宕机不可用，查找集群内所有 Down 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
- 当 down store 宕机不可用，查找集群内所有 Down 副本数小于正常副本数（少数派副本 DOWN）的所有 region 信息，并产生修复 remove-peer 修复建议
- 查找某个 region 信息
```
示例：
- 查找集群内只有指定副本数的所有 region 信息
$ ./tidba cluster region -u {user} -p {password} --host {host} -P {port} --peers {peerNums} --pdAddr {pdAddr:serviePort} --regiontype {all/data/index}

- 当 down tikv 宕机不可用，查找集群内所有 down-peer 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
假设三副本集群，存在 kv 节点 1,2,28,30，若 down store 1,28 宕机不可用，查找指定 down kv 节点集群内所有 Down 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
$ ./tidba cluster region -u {user} -p {password} --host {host} -P {port} --dowtikv {tikv1:servicePort1,tikv2:servicePort2} --pdAddr {pdAddr:serviePort} --regiontype {all/data/index} --replicatype majorDown

- 当 down tikv 宕机不可用，查找集群内所有 down-peer 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
假设三副本集群，存在 kv 节点 down，查找集群内所有 Down 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
$ ./tidba cluster region -u {user} -p {password} --host {host} -P {port} --dowtikv {tikv1:servicePort1,tikv2:servicePort2} --pdAddr {pdAddr:serviePort} --regiontype {all/data/index} --replicatype majorDown

- 当 down tikv 宕机不可用，查找集群内所有 down-peer 副本数小于正常副本数（少数派副本 DOWN）的所有 region 信息，并产生修复 remove-peer 修复建议
假设三副本集群，存在 kv 节点 1,2,28,30，若 down store 1,28 宕机不可用，查找集群内所有 Down 副本数小雨正常副本数（少数派副本 DOWN）的所有 region 信息
$ ./tidba cluster region -u {user} -p {password} --host {host} -P {port} --downtikv {tikv1:servicePort1,tikv2:servicePort2} --pdAddr {pdAddr:serviePort} --regiontype {all/data/index} --replicatype lessDown

-- 查找某个 region 信息
$ ./tidba cluster region -u {user} -p {password} --host {host} -P {port} --pdAddr {pdAddr:serviePort} --regionid {region1,region2,region3} 

```
table 功能集：
- 固定间隔，并发收集统计信息
- 查找表数据 region leader 以及索引 region leader 分布
- 查找某个表上的读写热点 region top 分布
```
示例：
-- 不间断并发搜集统计信息
$ ./tidba table analyze --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1,table2}

-- 查看数据以及索引 region leader 分布
$ ./tidba table region --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
$ ./tidba table region --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
--index {indexName}
$ ./tidba table region --host {host} --port {port} --user {user} --password {password} --db {dbName} --all

- 查找某个表上的读写热点 region top 分布
$ ./tidba table hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
 --hottype write --limit 10
$ ./tidba table hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
 --hottype read --limit 10
$ ./tidba table hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
 --hottype write --index {indexName1,indexName2} --limit 10
$ ./tidba table hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} -i {table1}
 --hottype read --index {indexName1,indexName2} --limit 10
```
store 功能集：
- 查找某个 store 上的读写热点 region top 分布
```
示例：
- 查找所有 store 某个 DB 上的读写热点 region top 分布
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} --hottype read --limit 10 --all
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} --db {dbName} --hottype write --limit 10 --all

- 查找所有 store 所有 DB 上的读写热点 region top 分布
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} --hottype read --limit 10 --all
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} --hottype write --limit 10 --all 
 
- 查找某个 store 上的读写热点 region top 分布
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} -i {storeID}
 --hottype read --limit 10
$ ./tidba store hotspot --host {host} --port {port} --user {user} --password {password} -i {storeID}
 --hottype write --limit 10
```
mok 功能集：
- Mok 解析 TiDB key, 集成包含原 Mok 工具
```
示例：
解析 tidb key
$ ./tidba mok 7A7480000000000007FF8F5F728000000000FF083BBA0000000000FAFA6C400A6673FFFE
"7A7480000000000007FF8F5F728000000000FF083BBA0000000000FAFA6C400A6673FFFE"
├─hex
│ └─"zt\200\000\000\000\000\000\007\377\217_r\200\000\000\000\000\377\010;\272\000\000\000\000\000\372\372l@\nfs\377\376"
│   └─rocksdb
│     └─"t\200\000\000\000\000\000\007\377\217_r\200\000\000\000\000\377\010;\272\000\000\000\000\000\372\372l@\nfs\377\376"
│       └─comparable
│         ├─"t\200\000\000\000\000\000\007\217_r\200\000\000\000\000\010;\272"
│         │ └─rowkey
│         │   ├─table: 1935
│         │   └─row: 539578
│         └─ts: 401875853330087937 (2018-07-31 18:58:38.819 +0800 CST)
└─base64
  └─"\354\016\370\363M4\323M4\323M4\323\261E\360^E\357o4\323M4\323M4\024]<\334\020@\323M4\323M4\323A@\024\016\202\34
```
exporter 功能集：
- 用户创建以及授权信息导出
```
示例：
-- 导出所有非 root 用户创建以及授权 SQL
$ ./tidba exporter user --host {host} --port {port} --user {user} --password {password} --cluster-version {clusterVersion 2/3/4/5} --all-user --out-dir {outDir}

-- 导出用户创建以及授权 SQL
$ ./tidba exporter user --host {host} --port {port} --user {user} --password {password} --cluster-version {clusterVersion 2/3/4/5} --include-user {user1,user2} --out-dir {outDir}
```
sql 功能集：
- SQL语句批量执行
```
示例：
-- 批量执行 SQL 
$ ./tidba exporter user --host {host} --port {port} --user {user} --password {password} --db {dbName} --sql-file {sql_store_dir} --sql-separator {sql-separator, default ;} --out-dir {outDir}
```

