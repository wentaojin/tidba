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
package runaway

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/xid"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/utils/stringutil"
)

func PrintSqlRunawayComment() {
	fmt.Println("NOTES：")
	fmt.Println("- 基于 SQL Digest 自动进行集群级别 SQL 限流 / SQL 拦截 KILL，以避免同类 SQL 影响集群整体性能")
}

func TopsqlRunaway(ctx context.Context, clusterName string, resourceGroup, sqlDigest, priority, sqlText, action string, ruPerSec int) ([]string, []map[string]string, error) {
	metaDB, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
	if err != nil {
		return nil, nil, err
	}
	meta := metaDB.(*sqlite.Database)

	rc, err := meta.GetResourceGroup(ctx, clusterName)
	if err != nil {
		return nil, nil, err
	}

	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return nil, nil, err
	}
	db := connDB.(*mysql.Database)

	_, res, err := db.GeneralQuery(ctx, `select version() AS VERSION`)
	if err != nil {
		return nil, nil, err
	}
	vers := strings.Split(res[0]["VERSION"], "-")

	if stringutil.VersionOrdinal(strings.TrimPrefix(vers[len(vers)-1], "v")) < stringutil.VersionOrdinal("8.5.0") {
		return nil, nil, fmt.Errorf("the cluster [%s] database version [%s] not meet requirement, require version >= v8.5.0, need use SWITCH_GROUP feature", clusterName, vers[len(vers)-1])
	}

	_, res, err = db.GeneralQuery(ctx, `select NAME from information_schema.resource_groups`)
	if err != nil {
		return nil, nil, err
	}

	var dbRgNames []string
	isExistedRc := false

	for _, r := range res {
		// exclude switch resource group name
		if r["NAME"] == rc.ResourceGroupName {
			isExistedRc = true
			continue
		}
		dbRgNames = append(dbRgNames, r["NAME"])
	}

	var limitSql []string

	if strings.EqualFold(action, "kill") {
		if resourceGroup != "" {
			if sqlDigest != "" {
				limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD RESOURCE GROUP %s ACTION KILL SQL DIGEST '%s'", resourceGroup, sqlDigest))
			}
			if sqlText != "" {
				limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD RESOURCE GROUP %s ACTION KILL SQL TEXT EXACT TO "%s"`, resourceGroup, sqlText))
			}
		} else {
			for _, r := range dbRgNames {
				if r == "default" {
					if sqlDigest != "" {
						limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD ACTION KILL SQL DIGEST '%s'", sqlDigest))
					}
					if sqlText != "" {
						limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD ACTION KILL SQL TEXT EXACT TO "%s"`, sqlText))
					}
				} else {
					if sqlDigest != "" {
						limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD RESOURCE GROUP %s ACTION KILL SQL DIGEST '%s'", r, sqlDigest))
					}
					if sqlText != "" {
						limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD RESOURCE GROUP %s ACTION KILL SQL TEXT EXACT TO "%s"`, r, sqlText))
					}
				}
			}
		}
	} else {
		var rcName string
		// not found record
		if rc.ResourceGroupName == "" {
			rcName = xid.New().String()
			if _, err := meta.CreateResourceGroup(ctx, &sqlite.ResourceGroup{
				ClusterName:       clusterName,
				ResourceGroupName: rcName,
			}); err != nil {
				return nil, nil, err
			}
		} else {
			rcName = rc.ResourceGroupName
		}

		if !isExistedRc {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE RESOURCE GROUP IF NOT EXISTS %s RU_PER_SEC = %d PRIORITY=%s", rcName, ruPerSec, priority)); err != nil {
				return nil, nil, err
			}
		}

		if resourceGroup != "" {
			if sqlDigest != "" {
				limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD RESOURCE GROUP %s ACTION SWITCH_GROUP(%s) SQL DIGEST '%s'", resourceGroup, rcName, sqlDigest))
			}
			if sqlText != "" {
				limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD RESOURCE GROUP %s ACTION SWITCH_GROUP(%s) SQL TEXT EXACT TO "%s"`, resourceGroup, rcName, sqlText))
			}
		} else {
			for _, r := range dbRgNames {
				if r == "default" {
					if sqlDigest != "" {
						limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD ACTION SWITCH_GROUP(%s) SQL DIGEST '%s'", rcName, sqlDigest))
					}
					if sqlText != "" {
						limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD ACTION SWITCH_GROUP(%s) SQL TEXT EXACT TO "%s"`, rcName, sqlText))
					}
				} else {
					if sqlDigest != "" {
						limitSql = append(limitSql, fmt.Sprintf("QUERY WATCH ADD RESOURCE GROUP %s ACTION SWITCH_GROUP(%s) SQL DIGEST '%s'", resourceGroup, rcName, sqlDigest))
					}
					if sqlText != "" {
						limitSql = append(limitSql, fmt.Sprintf(`QUERY WATCH ADD RESOURCE GROUP %s ACTION SWITCH_GROUP(%s) SQL TEXT EXACT TO "%s"`, resourceGroup, rcName, sqlText))
					}
				}
			}
		}
	}

	for _, s := range limitSql {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return nil, nil, fmt.Errorf("the query sql [%s] run failed: %v", s, err)
		}
	}

	cols, res, err := db.GeneralQuery(ctx, `SELECT
rw.ID,
rw.RESOURCE_GROUP_NAME AS RESOURCE_GROUP,
rw.START_TIME,
rw.END_TIME,
rg.RU_PER_SEC,
rg.PRIORITY,
rg.QUERY_LIMIT,
rw.ACTION,
rw.WATCH_TEXT
FROM 
INFORMATION_SCHEMA.RUNAWAY_WATCHES rw,
INFORMATION_SCHEMA.RESOURCE_GROUPS rg
WHERE
rw.RESOURCE_GROUP_NAME = rg.NAME`)
	if err != nil {
		return nil, nil, err
	}
	return cols, res, nil
}
