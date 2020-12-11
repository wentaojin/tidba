# TiDBA

TiDB 数据库 DBA 常用工具集，功能：

- 固定间隔，并发收集统计信息
- 查看表数据 region leader 以及索引 region leader 分布
- Mok 解析 TiDB key, 集成包含原 Mok 工具
- 基于 region key、数据 range、数据估算生成打散语句，并每种打散方式资源消耗以及场景可能情况不同，子命令分为基于实际数据表打散（key、range）、基于采样基础表打散（estimate、sampling、reckon）
- 版本升级，集群组件配置文件以及 tidb 专有系统变量 Diff 对比，现阶段只支持 3.0 diff 4.0 或者 4.0 (及以上版本) diff 4.0 (及以上版本)
  - v4.0 以下 tikv 需要单独从 tikv 日志中获取配置文件 --base-tikv-json 用于对比
  - v4.0 及以上可不需要，直接对比 --base-tikv-addr 、--new-tikv-addr 
- 导出用户创建以及授权信息
- Oracle 自动迁移 Schema 至 Mysql
  - 考虑 oracle 特有依赖, migrate 单独 repo: https://github.com/WentaoJin/transferdb

mok

```
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

table

```
-- 查看数据以及索引 region leader 分布
$ ./tidba table region --help

-- 不间断并发搜集统计信息
$ ./tidba table analyze --help
```

split

```
-- 基于已有数据，根据 range 生成打散
$./tidba split range --help

-- 基于已有数据，根据 key 生成打散
$./tidba split key --help

-- 基于基础采样表，根据 distinct 生成打散（因子放大倍数，数据量大的情况下资源耗用主要在于机器资源且比较大）
$./tidba split estimate --help

-- 基于基础采样表，根据 distinct 生成打散（数据量大的情况下,资源耗用主要在于机器资源且相对折中）
$./tidba split sampling --help

-- 基于基础采样表，根据 distinct 生成打散（数据量大的情况下,资源耗用主要位于数据库）
$./tidba split reckon --help

```

diff

```
-- 对比 pd 配置文件参数
$ ./tidba diff pd --help

-- 对比 tidb 配置文件以及专有系统变量参数
$ ./tidba diff tidb --help

-- 对比 tikv 配置文件参数
$ ./tidba diff tikv --help

v3.0 json 获取 tikv 日志配置（只有 v3.0 版本需要）,将 config 等于号后面的双引号中的内容全部复制倒另外一个文件内（注意不能带有双引号复制），用于 diff 对比
[2020/12/05 14:29:53.717 +08:00] [INFO] [server.rs:51] ["using config"] [config="{\"log-level\":\"info\",\"log-file\":\"/data1/nightly/tikv-41260/log/tikv.log\",....}"]
```

exporter

```
-- 导出用户创建以及授权 SQL
$./tidba exporter user --help
```

migrate - 考虑 oracle 特有依赖, migrate 单独 repo 

```
Oracle 自动迁移 Schema 至 Mysql repo
https://github.com/WentaoJin/transferdb
```



