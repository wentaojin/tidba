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
package database

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/database/sqlite"
)

var Connector *DBConnector

const (
	DefaultSqliteClusterName = "metadata"
	DatabaseTypeSqlite       = "sqlite"
	DatabaseTypeMySQL        = "mysql"
)

type Database interface {
	GetDatabase() interface{}
	CloseDatabase() error
}

type ClusterConfig struct {
	DbType string
	DSN    string
}

func CreateConnector(ctx context.Context, config *ClusterConfig) (Database, error) {
	switch config.DbType {
	case "sqlite":
		return sqlite.NewDatabase(config.DSN)
	case "mysql":
		return mysql.NewDatabase(ctx, config.DSN)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.DbType)
	}
}

type DBConnector struct {
	dbConns sync.Map // key: cluster_name, value: database struct
}

func NewDBConnector() *DBConnector {
	return &DBConnector{}
}

func (dbm *DBConnector) AddDatabase(clusterName string, database Database) {
	dbm.dbConns.Store(clusterName, database)
}

func (dbm *DBConnector) LoadDatabase(clusterName string) (Database, bool) {
	conn, ok := dbm.dbConns.Load(clusterName)
	if ok {
		return conn.(Database), true
	}

	return nil, false
}

func (dbm *DBConnector) createDatabaseConnector(ctx context.Context, clusterName string) (Database, error) {
	if clusterName == "" {
		return nil, fmt.Errorf("the cluster_name not configure value, not meet your expectations, please contact the author or try again")
	}
	db, ok := dbm.LoadDatabase(DefaultSqliteClusterName)
	if !ok {
		return nil, fmt.Errorf("invalid cluster [%s] database connector: not found metadata connector", DefaultSqliteClusterName)
	}
	c, err := db.(*sqlite.Database).GetCluster(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(c, &sqlite.Cluster{}) {
		return nil, fmt.Errorf("the cluster_name [%s] not found, please run [meta create] command used to create", clusterName)
	}

	connector, err := CreateConnector(ctx, &ClusterConfig{
		DbType: DatabaseTypeMySQL,
		DSN:    mysql.BuildDatabaseDSN(c.DbUser, c.DbPassword, c.DbHost, c.DbPort, c.DbCharset, c.ConnParams),
	})
	if err != nil {
		return nil, err
	}
	dbm.AddDatabase(clusterName, connector)

	return connector, nil
}

func (dbm *DBConnector) GetDatabase(clusterName string) (Database, error) {
	if dbm == nil {
		return nil, fmt.Errorf("database connector is not initialized")
	}
	conn, ok := dbm.LoadDatabase(clusterName)
	if !ok {
		newConn, err := dbm.createDatabaseConnector(context.Background(), clusterName)
		if err != nil {
			return nil, err
		}
		return newConn, nil
	}
	return conn, nil
}

func (dbm *DBConnector) GetNonMetadataClusters() []string {
	var keys []string
	dbm.dbConns.Range(func(key, value interface{}) bool {
		if key.(string) != DefaultSqliteClusterName {
			keys = append(keys, key.(string))
		}
		return true
	})
	return keys
}

func (dbm *DBConnector) CloseDatabase(clusterName string) error {
	conn, ok := dbm.dbConns.Load(clusterName)
	if !ok {
		return fmt.Errorf("the database [%s] connector not found, not need close database", clusterName)
	}
	if err := conn.(Database).CloseDatabase(); err != nil {
		return err
	}
	dbm.dbConns.Delete(clusterName)
	return nil
}

func LoginClusterDatabase(ctx context.Context, clusterName string) ([]*sqlite.Cluster, error) {
	var (
		datas []*sqlite.Cluster
		err   error
	)
	db, err := Connector.GetDatabase(DefaultSqliteClusterName)
	if err != nil {
		return datas, fmt.Errorf("invalid cluster [%s] database connector: %v", DefaultSqliteClusterName, err)
	}
	c, err := db.(*sqlite.Database).GetCluster(ctx, clusterName)
	if err != nil {
		return datas, err
	}
	if !reflect.DeepEqual(c, &sqlite.Cluster{}) {
		datas = append(datas, c)
	} else {
		return datas, fmt.Errorf("the cluster_name [%s] not found, please run [meta create] command used to create", clusterName)
	}

	connector, err := CreateConnector(ctx, &ClusterConfig{
		DbType: DatabaseTypeMySQL,
		DSN:    mysql.BuildDatabaseDSN(c.DbUser, c.DbPassword, c.DbHost, c.DbPort, c.DbCharset, c.ConnParams),
	})
	if err != nil {
		return datas, err
	}
	Connector.AddDatabase(clusterName, connector)
	return datas, err
}
