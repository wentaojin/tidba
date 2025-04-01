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

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/utils/stringutil"
)

func PrintSqlRunawayComment() {
	fmt.Println("NOTES：")
	fmt.Println("- 基于 SQL Digest 自动进行集群级别 SQL 限流，以避免同类 SQL 影响集群整体性能")
}

func TopsqlRunaway(ctx context.Context, clusterName string, resourceGroup, sqlDigest, priority string, ruPerSec int) ([]string, []map[string]string, error) {
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
	for _, r := range res {
		dbRgNames = append(dbRgNames, r["NAME"])
	}

	if stringutil.IsContainStringIgnoreCase(resourceGroup, dbRgNames) {
		return nil, nil, fmt.Errorf("the configuration --resource-group [%s] has existed, please re-specify the name of the resource group, the database resource groups: [%v]", resourceGroup, strings.Join(dbRgNames, ","))
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE RESOURCE GROUP %s RU_PER_SEC = %d PRIORITY=%s", resourceGroup, ruPerSec, priority)); err != nil {
		return nil, nil, err
	}

	var limitSql []string
	for _, r := range dbRgNames {
		if r == "default" {
			limitSql = append(limitSql, fmt.Sprintf("query watch add action switch_group(%s) sql digest '%s'", resourceGroup, sqlDigest))
		} else {
			limitSql = append(limitSql, fmt.Sprintf("query watch add resource group %s action switch_group(%s) sql digest '%s'", r, resourceGroup, sqlDigest))
		}
	}

	for _, s := range limitSql {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return nil, nil, err
		}
	}

	cols, res, err := db.GeneralQuery(ctx, `SELECT * FROM INFORMATION_SCHEMA.RUNAWAY_WATCHES ORDER BY id`)
	if err != nil {
		return nil, nil, err
	}
	return cols, res, nil
}
