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
package table

import (
	"fmt"
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/util"
	"golang.org/x/sync/errgroup"
)

func IncludeTableAnalyze(dbName string, concurrency int, includeTables []string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	isSubset, notExistTables := util.IsExistInclude(allTables, includeTables)
	if !isSubset {
		return fmt.Errorf("db %s table '%v' not exist", dbName, notExistTables)
	}

	// concurrency analyze
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range includeTables {
		sql := fmt.Sprintf("analyze table %s.%s", dbName, t)
		g.Go(func() error {
			if err := tableAnalyze(engine, sql); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func FilterTableAnalyze(dbName string, concurrency int, excludeTables []string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	// concurrency analyze
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range util.FilterFromAll(allTables, excludeTables) {
		sql := fmt.Sprintf("analyze table %s.%s", dbName, t)
		g.Go(func() error {
			if err := tableAnalyze(engine, sql); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func RegexpTableAnalyze(dbName string, concurrency int, regex string, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	// concurrency analyze
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range util.RegexpFromAll(allTables, regex) {
		sql := fmt.Sprintf("analyze table %s.%s", dbName, t)
		g.Go(func() error {
			if err := tableAnalyze(engine, sql); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func AllTableAnalyze(dbName string, concurrency int, engine *db.Engine) error {
	allTables, err := engine.GetAllTables(dbName)
	if err != nil {
		return err
	}

	// concurrency analyze
	g := &errgroup.Group{}
	g.SetLimit(concurrency)

	for _, t := range allTables {
		sql := fmt.Sprintf("analyze table %s.%s", dbName, t)
		g.Go(func() error {
			if err := tableAnalyze(engine, sql); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// tableAnalyze is used to analyze table
func tableAnalyze(db *db.Engine, sql string) (err error) {
	_, err = db.MySQLDB.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}
