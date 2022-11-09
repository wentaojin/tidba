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
package cmd

import (
	"fmt"
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/table"
	"time"

	"github.com/spf13/cobra"
)

// AppTable is storage for the sub command analyze
type AppTable struct {
	*App          // embedded parent command storage
	host          string
	port          int
	user          string
	password      string
	dbName        string
	all           bool
	includeTables []string
	excludeTables []string
	regexTables   string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *App) AppTable() Cmder {
	return &AppTable{App: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppTable) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "table",
		Short:            "Operator database table",
		Long:             `Operator database table`,
		RunE:             app.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.PersistentFlags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.dbName, "db", "D", "", "database name")
	cmd.PersistentFlags().StringSliceVarP(&app.includeTables, "include", "i", nil, "configure table name")
	cmd.PersistentFlags().StringSliceVarP(&app.excludeTables, "exclude", "e", nil, "configure exclude table name")
	cmd.PersistentFlags().StringVarP(&app.regexTables, "regex", "r", "", "configure table name by go regexp")
	cmd.PersistentFlags().BoolVar(&app.all, "all", false, "all tables in the database")
	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppTable) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
	Analyze table
*/
// includeTable、excludeTable、regexTable only one of the three
type AppTableAnalyze struct {
	*AppTable
	Interval int // app run interval
}

// is a method of App that returns a Cmder instance for the sub command
func (app *AppTable) AppTableAnalyze() Cmder {
	return &AppTableAnalyze{AppTable: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppTableAnalyze) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "analyze",
		Short:        "Analyze database table statistics",
		Long:         `Analyze database table statistics`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVar(&app.Interval, "interval", 5, "analyze run interval/s")

	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppTableAnalyze) RunE(cmd *cobra.Command, args []string) error {
	if app.dbName == "" {
		return fmt.Errorf("flag db name is requirement, can not null")
	}
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.dbName) {
		return err
	}

	if app.all {
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.AllTableAnalyze(app.dbName, app.Concurrency, engine); err != nil {
				return err
			}
		}
	}

	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.IncludeTableAnalyze(app.dbName, app.Concurrency, app.includeTables, engine); err != nil {
				return err
			}
		}
	case app.includeTables == nil && app.excludeTables != nil && app.regexTables == "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.FilterTableAnalyze(app.dbName, app.Concurrency, app.excludeTables, engine); err != nil {
				return err
			}
		}
	case app.includeTables == nil && app.excludeTables == nil && app.regexTables != "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.RegexpTableAnalyze(app.dbName, app.Concurrency, app.regexTables, engine); err != nil {
				return err
			}
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}
	return nil
}

/*
	Table data or index region leader distribution
*/
// includeTable、excludeTable、regexTable only one of the three
type AppTableRegion struct {
	*AppTable
	indexName []string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *AppTable) AppTableRegion() Cmder {
	return &AppTableRegion{AppTable: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppTableRegion) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "region",
		Short:        "View table data or index region leader distribution",
		Long:         `View table data or index region leader distribution`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringSliceVar(&app.indexName, "index", nil, "configure table index name")

	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppTableRegion) RunE(cmd *cobra.Command, args []string) error {
	if app.dbName == "" {
		return fmt.Errorf("flag db name is requirement, can not null")
	}
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.dbName) {
		return err
	}

	if app.all {
		if err := table.AllTableRegionDataView(app.dbName, app.Concurrency, engine); err != nil {
			return err
		}
		if err := table.AllTableRegionIndexView(app.dbName, app.Concurrency, app.indexName, engine); err != nil {
			return err
		}
	}
	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "" && app.indexName == nil:
		if err := table.IncludeTableRegionDataView(app.dbName, app.Concurrency, app.includeTables, engine); err != nil {
			return err
		}

	case app.includeTables == nil && app.excludeTables != nil && app.regexTables == "" && app.indexName == nil:
		if err := table.FilterTableRegionDataView(app.dbName, app.Concurrency, app.excludeTables, engine); err != nil {
			return err
		}

	case app.includeTables == nil && app.excludeTables == nil && app.regexTables != "" && app.indexName == nil:
		if err := table.RegexpTableRegionDataView(app.dbName, app.Concurrency, app.regexTables, engine); err != nil {
			return err
		}

	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "" && app.indexName != nil:
		if err := table.IncludeTableRegionIndexView(app.dbName, app.Concurrency, app.includeTables, app.indexName, engine); err != nil {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}

/*
	Table data or index region hotspot top distribution
*/
// includeTable、excludeTable、regexTable only one of the three
type AppTableHotspot struct {
	*AppTable
	hotType   string
	limit     int
	indexName []string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *AppTable) AppTableHotspot() Cmder {
	return &AppTableHotspot{AppTable: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppTableHotspot) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "hotspot",
		Short:        "View table data or index region hotspot top distribution",
		Long:         `View table data or index region hotspot top distribution`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&app.hotType, "hottype", "t", "read", "configure view hotspot type (read/write)")
	cmd.Flags().IntVarP(&app.limit, "limit", "l", 10, "configure view hotspot top limit")
	cmd.Flags().StringSliceVar(&app.indexName, "index", nil, "configure table index name")

	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppTableHotspot) RunE(cmd *cobra.Command, args []string) error {
	if app.dbName == "" {
		return fmt.Errorf("flag db name is requirement, can not null")
	}
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.dbName) {
		return err
	}

	if app.all {
		if err := table.AllTableDataAndIndexRegionHotspotView(app.dbName, app.hotType, app.limit, engine); err != nil {
			return err
		}
		return nil
	}
	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "" && app.indexName == nil:
		if err := table.IncludeTableDataRegionHotspotView(app.dbName, app.hotType, app.limit, app.Concurrency, app.includeTables, engine); err != nil {
			return err
		}

	case app.includeTables == nil && app.excludeTables != nil && app.regexTables == "" && app.indexName == nil:
		if err := table.FilterTableDataRegionHotspotView(app.dbName, app.hotType, app.limit, app.Concurrency, app.excludeTables, engine); err != nil {
			return err
		}

	case app.includeTables == nil && app.excludeTables == nil && app.regexTables != "" && app.indexName == nil:
		if err := table.RegexpTableDataRegionHotspotView(app.dbName, app.hotType, app.limit, app.Concurrency, app.regexTables, engine); err != nil {
			return err
		}

	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "" && app.indexName != nil:
		if err := table.IncludeTableIndexRegionHotspotView(app.dbName, app.hotType, app.limit, app.Concurrency, app.includeTables, app.indexName, engine); err != nil {
			return err
		}

	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
