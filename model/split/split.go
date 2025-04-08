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
package split

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/logger"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

type SplitRespMsg struct {
	Err error
}

type SplitReqMsg struct {
	Database    string
	Tables      []string
	Concurrency int
	Output      string
	Command     string

	// sampling
	IndexName string
	Resource  string

	// estimate
	ColumnNames  []string
	EstimateSize int
	RegionSize   int

	NewDbName    string
	NewTableName string
	NewIndexName string
	EstimateRows int
}

func RunTableSplitTask(ctx context.Context, clusterName string, req *SplitReqMsg) error {
	taskStart := time.Now()
	logger.Info(fmt.Sprintf("Cluster [%s] database [%s] connection get...", clusterName, req.Database))
	conn, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return err
	}
	db := conn.(*mysql.Database)

	logger.Info(fmt.Sprintf("Cluster [%s] database [%s] table get...", clusterName, req.Database))
	schemas, err := db.GetDatabases(ctx)
	if err != nil {
		return err
	}
	if !stringutil.IsContainStringIgnoreCase(req.Database, schemas) {
		return fmt.Errorf("the database name [%s] was not found in the cluster [%s]", req.Database, clusterName)
	}
	dbTables, err := db.GetDatabaseTables(ctx, req.Database)
	if err != nil {
		return err
	}
	differs := stringutil.NewStringSet(req.Tables...).Difference(stringutil.NewStringSet(dbTables...)).Slice()
	if len(differs) > 0 {
		return fmt.Errorf("the tables [%v] was not found in the cluster [%s] database [%s]", differs, req.Database, clusterName)
	}

	var tasks []*Task
	for _, t := range req.Tables {
		tasks = append(tasks, &Task{
			Mutex:     &sync.Mutex{},
			Command:   req.Command,
			DbName:    req.Database,
			TableName: t,
			OutDir:    req.Output,
			Engine:    db,
		})
	}

	logger.Info(fmt.Sprintf("Cluster [%s] database [%s] table counts [%d]...", clusterName, req.Database, len(tasks)))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(req.Concurrency)

	for _, task := range tasks {
		t := task
		g.Go(func() error {
			switch strings.ToUpper(req.Command) {
			case "RANGE":
				if _, err := t.RangeCommand(gCtx); err != nil {
					return err
				}
			case "KEY":
				if _, err := t.KeyCommand(gCtx); err != nil {
					return err
				}
			case "SAMPLING":
				if _, err := t.SamplingCommand(gCtx, req.IndexName, req.NewDbName, req.NewTableName, req.NewIndexName, req.Resource, req.EstimateRows); err != nil {
					return err
				}
			case "ESTIMATE":
				if _, err := t.EstimateCommand(gCtx, req.ColumnNames, req.NewDbName, req.NewTableName, req.NewIndexName, req.EstimateRows, req.EstimateSize, req.Concurrency, req.RegionSize); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown command: [%s]", req.Command)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Cluster [%s] database [%s] split task completed in %fs", clusterName, req.Database, time.Since(taskStart).Seconds()))
	logger.Info(fmt.Sprintf("Cluster [%s] database [%s] split task output dir to [%s]", clusterName, req.Database, req.Output))
	return nil
}
