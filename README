# TiDBA

TiDB 数据库 DBA 常用工具集，包含收集统计信息、Mok 解析 key、基于 region key、数据 range、数据估算生成打散语句、查看表 region 分布、Oracle 自动迁移 Schema 至 Mysql 等功能。

```
./tidba --help
CLI tidba app for tidb cluster

Usage:
  tidba [command]

Available Commands:
  help        Help about any command
  migrate     Migrate oracle schema to mysql
  mok         Mok used to decode or build tidb key
  split       Split region used to scatter hot key
  table       Operator database table

Flags:
      --all               all tables in the database
  -f, --concurrency int   app concurrency (default 5)
  -d, --db string         database name
  -e, --exclude strings   configure exclude table name
  -h, --help              help for tidba
      --host string       database host ip (default "127.0.0.1")
  -i, --include strings   configure table name
  -p, --password string   database user password
  -P, --port string       database service port (default "4000")
  -r, --regex string      configure table name by go regexp
  -u, --user string       database user name (default "root")
  -v, --version string    version for app

Use "tidba [command] --help" for more information about a command.
```

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

split

```
基于数据范围
./tidba split range --help
Split region base region range

Usage:
  tidba split range [flags]

Flags:
  -h, --help             help for range
  -o, --out-dir string   split sql file output dir (default "/tmp/split")

Global Flags:
      --all               all tables in the database
  -f, --concurrency int   app concurrency (default 5)
  -d, --db string         database name
  -e, --exclude strings   configure exclude table name
      --host string       database host ip (default "127.0.0.1")
  -i, --include strings   configure table name
  -p, --password string   database user password
  -P, --port string       database service port (default "4000")
  -r, --regex string      configure table name by go regexp
  -u, --user string       database user name (default "root")
  -v, --version string    version for app

基于估算
./tidba split estimate --help
Split non-associated index region base estimate data

Usage:
  tidba split estimate [flags]

Flags:
      --col string           configure base estimate table column name
  -h, --help                 help for estimate
      --new-db string        configure generate split table new db name through base estimate table column name
      --new-index string     configure generate split table index name through base estimate table column name
      --new-table string     configure generate split table new table name through base estimate table column name
      --new-table-row int    estimate need be split table rows
      --new-table-size int   estimate need be split table size(M)
  -o, --out-dir string       split sql file output dir (default "/tmp/split")
      --region-size int      estimate need be split table region size(M) (default 96)

Global Flags:
      --all               all tables in the database
  -f, --concurrency int   app concurrency (default 5)
  -d, --db string         database name
  -e, --exclude strings   configure exclude table name
      --host string       database host ip (default "127.0.0.1")
  -i, --include strings   configure table name
  -p, --password string   database user password
  -P, --port string       database service port (default "4000")
  -r, --regex string      configure table name by go regexp
  -u, --user string       database user name (default "root")
  -v, --version string    version for app

基于数据 key
./tidba split key --help
Split region base region key

Usage:
  tidba split key [flags]

Flags:
  -h, --help              help for key
  -o, --out-dir string    split sql file output dir (default "/tmp/split")
      --status-port int   tidb server status port (default 10080)

Global Flags:
      --all               all tables in the database
  -f, --concurrency int   app concurrency (default 5)
  -d, --db string         database name
  -e, --exclude strings   configure exclude table name
      --host string       database host ip (default "127.0.0.1")
  -i, --include strings   configure table name
  -p, --password string   database user password
  -P, --port string       database service port (default "4000")
  -r, --regex string      configure table name by go regexp
  -u, --user string       database user name (default "root")
  -v, --version string    version for app
```

migrate - 考虑 oracle 特有依赖, migrate 单独 repo 

```
Oracle 自动迁移 Schema 至 Mysql repo
https://github.com/WentaoJin/transferdb
```



