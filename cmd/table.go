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
	"time"

	"github.com/WentaoJin/tidba/pkg/table"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/spf13/cobra"
)

// AppTable is storage for the sub command analyze
type AppTable struct {
	*App // embedded parent command storage
}

//  is a method of App that returns a Cmder instance for the sub command
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

//  is a method of App that returns a Cmder instance for the sub command
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
	if app.DBName == "" {
		return fmt.Errorf("flag db name is requirement, can not null")
	}
	engine, err := db.NewMysqlDSN(app.User, app.Password, app.Host, app.Port, app.DBName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.DBName) {
		return err
	}

	if app.All {
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.AllTableAnalyze(app.DBName, app.Concurrency, engine); err != nil {
				return err
			}
		}
	}

	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.IncludeTableAnalyze(app.DBName, app.Concurrency, app.IncludeTable, engine); err != nil {
				return err
			}
		}
	case app.IncludeTable == nil && app.ExcludeTable != nil && app.RegexTable == "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.FilterTableAnalyze(app.DBName, app.Concurrency, app.ExcludeTable, engine); err != nil {
				return err
			}
		}
	case app.IncludeTable == nil && app.ExcludeTable == nil && app.RegexTable != "":
		for range time.Tick(time.Duration(app.Interval) * time.Second) {
			if err := table.RegexpTableAnalyze(app.DBName, app.Concurrency, app.RegexTable, engine); err != nil {
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
	IndexName []string
}

//  is a method of App that returns a Cmder instance for the sub command
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
	cmd.Flags().StringSliceVar(&app.IndexName, "index", nil, "configure table index name")

	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppTableRegion) RunE(cmd *cobra.Command, args []string) error {
	if app.DBName == "" {
		return fmt.Errorf("flag db name is requirement, can not null")
	}
	engine, err := db.NewMysqlDSN(app.User, app.Password, app.Host, app.Port, app.DBName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.DBName) {
		return err
	}

	if app.All {
		if err := table.AllTableRegionDataView(app.DBName, app.Concurrency, engine); err != nil {
			return err
		}
		if err := table.AllTableRegionIndexView(app.DBName, app.Concurrency, app.IndexName, engine); err != nil {
			return err
		}
	}
	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "" && app.IndexName == nil:
		if err := table.IncludeTableRegionDataView(app.DBName, app.Concurrency, app.IncludeTable, engine); err != nil {
			return err
		}

	case app.IncludeTable == nil && app.ExcludeTable != nil && app.RegexTable == "" && app.IndexName == nil:
		if err := table.FilterTableRegionDataView(app.DBName, app.Concurrency, app.ExcludeTable, engine); err != nil {
			return err
		}

	case app.IncludeTable == nil && app.ExcludeTable == nil && app.RegexTable != "" && app.IndexName == nil:
		if err := table.RegexpTableRegionDataView(app.DBName, app.Concurrency, app.RegexTable, engine); err != nil {
			return err
		}

	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "" && app.IndexName != nil:
		if err := table.IncludeTableRegionIndexView(app.DBName, app.Concurrency, app.IncludeTable, app.IndexName, engine); err != nil {
			return err
		}

	case app.IncludeTable == nil && app.ExcludeTable != nil && app.RegexTable == "" && app.IndexName != nil:
		if err := table.FilterTableRegionIndexView(app.DBName, app.Concurrency, app.ExcludeTable, app.IndexName, engine); err != nil {
			return err
		}
	case app.IncludeTable == nil && app.ExcludeTable == nil && app.RegexTable != "" && app.IndexName != nil:
		if err := table.RegexpTableRegionIndexView(app.DBName, app.Concurrency, app.RegexTable, app.IndexName, engine); err != nil {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
