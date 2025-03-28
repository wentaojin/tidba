# TiDBA

TiDB 数据库 DBA 运维瑞士军刀（交互式 / 非交互式 CLI），涵盖数据库自动化巡检、故障应急诊断、批量写入热点打散、参数变量对比等常见运维命令集。

**NOTES:**
1. 使用时必须跟 TiUP 中控机所在用户环境 Shell 保持一致，即沿用 TiUP 部分元数据信息
2. 使用前必须执行 meta create 命令创建集群访问所需元数据，clusterName 需要与 TiUP 集群名保持一致
   ```
    {                                     
    "clusterName": "",                   
    "dbUser": "",                        
    "dbPassword": "",                    
    "dbHost": "",                        
    "dbPort": 0,                         
    "dbCharset": "utf8mb4",              
    "connParams": "",                    
    "comment": ""                        
    }   
   ```
3. 交互式命令行 CLI 可通过 login / logout 命令进行集群切换操作（默认以交互式 CLI 模式运行）
    ```
    login 前 CLI 显示状态
    tidba »»» login -c tidb-jwt00

    login 后 CLI 显示状态
    tidba[tidb-jwt00] »»» 
    ```
---

### Inspect 命令

inspect 命令功能集：
- inspect create 数据巡检参数配置创建
- inspect query  数据库巡检参数配置查询
- inspect update 数据库巡检参数配置修改
- inspect delete 数据库巡检参数配置删除
- inspect start 数据库巡检启动
  
```
示例：
非交互式命令
$ ./tidba inspect {subCommand} -c {clusterName}

交互式命令
tidba[tidb-jwt00] »»» inspect {subCommand}
```
### Split 命令

每种打散方式资源消耗以及场景可能情况不同，子命令分为基于实际数据表打散（key、range）、基于采样基础表打散(estimate、sampling)

split 命令功能集：
- split range 基于数据库表已有数据，自动生成数据以及索引打散语句
- split key 基于数据库表已有 region key，自动生成数据以及索引打散语句
- split estimate 采样指定数据库表某些字段 columns 所在数据，自动生成索引打散语句
- split sampling 采样指定数据表某个索引名 index 所在字段数据，自动生成索引打散语句

```
示例：
非交互式命令
基于已有数据，根据 range 生成打散
$ ./tidba split range -c {clusterName} --database {dbName} --tables {table1,table2} --daemon

基于已有数据，根据 key 生成打散
$ ./tidba split key -c {clusterName} --database {dbName} --tables {table1,table2} --daemon

基于基础采样表，根据 distinct 生成打散（因子放大倍数，数据量大的情况下资源耗用主要在于机器资源且比较大）
$ ./tidba split estimate -c {clusterName} --database {dbName} --tables {tableName} --columns {column1,column2} --new-db {newDbName} --new-table {newTableName} --new-index {newIndexName} --estimate-row {estimateRows} --estimate-size {estimateSize} --daemon

基于基础采样表，根据 distinct 生成打散
--resource server 数据量大的情况下,资源耗用主要在于机器资源且相对折中
--resource database 数据量大的情况下,资源耗用主要位于数据库
$ ./tidba split sampling  -c {clusterName} --database {dbName} --tables {tableName} --index {indexName} --new-db {newDbName} --new-table {newTableName} --new-index {newIndexName} --estimate-row {estimateRows} --resource server --daemon

交互式命令(除 --daemon 以及 tidba 字样之外其他保持一致)
tidba[tidb-jwt00] »»» split {subCommand} ...flags
```
### Region 命令

region 命令功能集：
- region hotspot 查找集群数据库热点 region（支持数据库、store 实例、数据表、数据索引级别）
- region replica 查找 down store 宕机不可用，集群内所有 down 副本数大于或等于正常副本数（多数派副本 DOWN）的 所有 region 信息
- region leader  查找集群表数据 region leader 或者索引 region leader 分布
- region query  查找集群指定 region id 信息，包含数据库、表、索引、decode key 值

```
示例：
非交互式命令

查找集群数据库热点 region 信息
$ ./tidba region hotspot -c {clusterName} --database {dbName} --stores {tikv1:servicePort1,tikv1:servicePort2} --tables {tableName} --indexes {indexName1,indexName2} --type {write/read/all} --top 10

查看数据以及索引 region leader 分布
$ ./tidba region leader -c {clusterName} --database {dbName} --stores {tikv1:servicePort1,tikv1:servicePort2} --tables {tableName} --indexes {indexName1,indexName2}

当 down tikv 宕机不可用，查找集群内所有 down-peer 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息【忽略已被删除但未 GC 的 Region 信息】
假设三副本集群，存在 kv 节点 1,2,28,30，若 down store 1,28 宕机不可用，查找指定 down kv 节点地址集群内所有 Down 副本数大于或等于正常副本数（多数派副本 DOWN）的所有 region 信息
$ ./tidba region replica -c {clusterName} --stores {tikv1:servicePort1,tikv2:servicePort2} --region-type {all/data/index} --daemon

-- 查找指定的 region 信息
$ ./tidba region query -c {clusterName} --region-ids {region1,region2,region3} 

交互式命令(除 --daemon 以及 tidba 字样之外其他保持一致)
tidba[tidb-jwt00] »»» split {subCommand} ...flags
```
### TOPSQL 命令

topsql 命令功能集（默认取近 30 min且不显示 sql 文本）：
- topsql elapsed 基于时间窗口内 sql 执行总耗时排序
- topsql plans 基于时间窗口内产生多个执行计划的 sql 排序
- topsql execs 基于时间窗口内 sql 执行次数排序，显示获取每类 sql 解析、编译、执行的平均时间
- topsql cpu 基于时间窗口内 tidb / tikv 组件或者实例集群 cpu 维度排序

```
示例：
非交互命令
$ ./tidba topsql elapsed -c {clusterName} [--nearly 30 / --start {startTime} --end {endTime} ] --top 10 [--enable-sql] [--enable-history]

$ ./tidba topsql plans -c {clusterName} [--nearly 30 / --start {startTime} --end {endTime} ] --top 10 [--enable-sql] [--enable-history]

$ ./tidba topsql execs -c {clusterName} [--nearly 30 / --start {startTime} --end {endTime} ] --top 10 [--enable-sql] [--enable-history]

$ ./tidba topsql cpu -c {clusterName} --component {tidb/tikv} [--instances {instAddr:statusPort}] [--nearly 30 / --start {startTime} --end {endTime} ] --top 10 [--enable-sql] [--enable-history]

交互式命令(除 tidba 字样之外其他保持一致)
tidba[tidb-jwt00] »»» topsql {subCommand} ...flags
```

