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
package sqlite

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Database struct {
	mutex sync.RWMutex
	DB    *gorm.DB
}

func NewDatabase(dbPath string) (*Database, error) {
	sqlitedb, err := gorm.Open(sqlite.Open(fmt.Sprintf("%s/tidba.db", dbPath)), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := sqlitedb.DB()
	if err != nil {
		return nil, err
	}

	// SetMaxIdleConns sets the maximum number of Databaseions in the idle Databaseion pool.
	sqlDB.SetMaxIdleConns(10)
	// SetMaxOpenConns sets the maximum number of open Databaseions to the database.
	sqlDB.SetMaxOpenConns(100)
	// SetConnMaxLifetime sets the maximum amount of time a Databaseion may be reused.
	sqlDB.SetConnMaxLifetime(time.Hour)

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("ping the sqlite database error: [%s]", err)
	}

	if err := sqlitedb.AutoMigrate(
		&Cluster{},
		&Inspect{},
	); err != nil {
		return nil, fmt.Errorf("migrate the sqlite table error: [%s]", err)
	}

	return &Database{DB: sqlitedb}, nil
}

func (d *Database) GetDatabase() interface{} {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d
}

func (d *Database) CloseDatabase() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	sqlDB, err := d.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (d *Database) ClusterTableName(ctx context.Context) string {
	return d.DB.NamingStrategy.TableName(reflect.TypeOf(Cluster{}).Name())
}

func (d *Database) CreateCluster(ctx context.Context, data *Cluster) (*Cluster, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err := d.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "cluster_name"}},
		UpdateAll: true,
	}).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", d.ClusterTableName(ctx), err)
	}
	return data, nil
}

func (d *Database) DeleteCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var data *Cluster
	if err := d.DB.Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&Cluster{}).Where("cluster_name = ?", clusterName).Find(&data).Limit(1).Error
		if err != nil {
			return fmt.Errorf("get table [%s] record failed: %v", d.ClusterTableName(ctx), err)
		}
		err = tx.Where("cluster_name = ?", clusterName).Delete(&Cluster{}).Error
		if err != nil {
			return fmt.Errorf("delete table [%s] record failed: %v", d.ClusterTableName(ctx), err)
		}
		return nil
	}); err != nil {
		return data, err
	}
	return data, nil
}

func (d *Database) UpdateCluster(ctx context.Context, clusterName string, updates map[string]interface{}) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err := d.DB.Model(&Cluster{}).Where("cluster_name = ?", clusterName).Updates(updates).Error
	if err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", d.ClusterTableName(ctx), err)
	}
	return nil
}

func (d *Database) GetCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var data *Cluster
	err := d.DB.Model(&Cluster{}).Where("cluster_name = ?", clusterName).Find(&data).Limit(1).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", d.ClusterTableName(ctx), err)
	}
	return data, nil
}

func (d *Database) ListCluster(ctx context.Context, page uint64, pageSize uint64) ([]*Cluster, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var dataS []*Cluster
	if page == 0 && pageSize == 0 {
		err := d.DB.Model(&Cluster{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", d.ClusterTableName(ctx), err)
		}
		return dataS, nil
	}
	err := d.DB.Scopes(Paginate(int(page), int(pageSize))).Model(&Cluster{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", d.ClusterTableName(ctx), err)
	}
	return dataS, nil
}

func (d *Database) InspectTableName(ctx context.Context) string {
	return d.DB.NamingStrategy.TableName(reflect.TypeOf(Inspect{}).Name())
}

func (d *Database) CreateInspect(ctx context.Context, data *Inspect) (*Inspect, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err := d.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "cluster_name"}},
		UpdateAll: true,
	}).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", d.InspectTableName(ctx), err)
	}
	return data, nil
}

func (d *Database) DeleteInspect(ctx context.Context, clusterName string) (*Inspect, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var data *Inspect
	if err := d.DB.Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&Inspect{}).Where("cluster_name = ?", clusterName).Find(&data).Limit(1).Error
		if err != nil {
			return fmt.Errorf("get table [%s] record failed: %v", d.InspectTableName(ctx), err)
		}
		err = tx.Where("cluster_name = ?", clusterName).Delete(&Inspect{}).Error
		if err != nil {
			return fmt.Errorf("delete table [%s] record failed: %v", d.InspectTableName(ctx), err)
		}
		return nil
	}); err != nil {
		return data, err
	}
	return data, nil
}

func (d *Database) UpdateInspect(ctx context.Context, clusterName string, updates map[string]interface{}) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err := d.DB.Model(&Inspect{}).Where("cluster_name = ?", clusterName).Updates(updates).Error
	if err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", d.InspectTableName(ctx), err)
	}
	return nil
}

func (d *Database) GetInspect(ctx context.Context, clusterName string) (*Inspect, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var data *Inspect
	err := d.DB.Model(&Inspect{}).Where("cluster_name = ?", clusterName).Find(&data).Limit(1).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", d.InspectTableName(ctx), err)
	}
	return data, nil
}
