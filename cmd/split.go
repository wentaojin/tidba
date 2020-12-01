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

	"github.com/WentaoJin/tidba/pkg/split"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/spf13/cobra"
)

// AppSplit is storage for the sub command analyze
// includeTable、excludeTable、regexTable only one of the three
type AppSplit struct {
	*App // embedded parent command storage
}

func (app *App) AppSplit() Cmder {
	return &AppSplit{App: app}
}

func (app *AppSplit) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "split",
		Short:        "Split region used to scatter hot key",
		Long:         `Split region used ti scatter hot key`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (app *AppSplit) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
	Base range split
*/
type AppSplitRange struct {
	*AppSplit // embedded parent command storage
	OutDir    string
}

func (app *AppSplit) AppSplitRange() Cmder {
	return &AppSplitRange{AppSplit: app}
}

func (app *AppSplitRange) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "range",
		Short:        "Split region base region range",
		Long:         `Split region base region range`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/split", "split sql file output dir")
	return cmd
}

func (app *AppSplitRange) RunE(cmd *cobra.Command, args []string) error {
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
		if err := split.AllTableSplitRange(app.DBName, app.Concurrency, app.OutDir, engine); err != nil {
			return err
		}
	}

	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "":
		if err := split.IncludeTableSplitRange(app.DBName, app.Concurrency, app.IncludeTable, app.OutDir, engine); err != nil {
			return err
		}
	case app.IncludeTable == nil && app.ExcludeTable != nil && app.RegexTable == "":
		if err := split.FilterTableSplitRange(app.DBName, app.Concurrency, app.IncludeTable, app.OutDir, engine); err != nil {
			return err
		}
	case app.IncludeTable == nil && app.ExcludeTable == nil && app.RegexTable != "":
		if err := split.RegexpTableSplitRange(app.DBName, app.Concurrency, app.RegexTable, app.OutDir, engine); err != nil {
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
	Base estimate split
*/

type AppSplitEstimate struct {
	*AppSplit         // embedded parent command storage
	EstimateTableRows int
	EstimateTableSize int
	RegionSize        int
	ColumnName        string
	NewDbName         string
	NewTableName      string
	NewIndexName      string
	OutDir            string
}

func (app *AppSplit) AppSplitEstimate() Cmder {
	return &AppSplitEstimate{AppSplit: app}
}

func (app *AppSplitEstimate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "estimate",
		Short:        "Split single and joint index region base estimate data",
		Long:         `Split single and joint index region base estimate data`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVar(&app.EstimateTableRows, "new-table-row", 0, "estimate need be split table rows")
	cmd.Flags().IntVar(&app.EstimateTableSize, "new-table-size", 0, "estimate need be split table size(M)")
	cmd.Flags().IntVar(&app.RegionSize, "region-size", 96, "estimate need be split table region size(M)")
	cmd.Flags().StringVar(&app.ColumnName, "col", "", "configure base estimate table column name")
	cmd.Flags().StringVar(&app.NewDbName, "new-db", "", "configure generate split table new db name through base estimate table column name")
	cmd.Flags().StringVar(&app.NewTableName, "new-table", "", "configure generate split table new table name through base estimate table column name")
	cmd.Flags().StringVar(&app.NewIndexName, "new-index", "", "configure generate split table index name through base estimate table column name")
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/split", "split sql file output dir")

	return cmd
}

func (app *AppSplitEstimate) RunE(cmd *cobra.Command, args []string) error {
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

	//only support single table
	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "":
		if len(app.IncludeTable) != 1 {
			return fmt.Errorf(" flag include only support configre single table")
		}
		if app.NewIndexName == "" {
			return fmt.Errorf("flag new index name is requirement, can not null")

		}
		if err := split.IncludeTableSplitEstimate(engine,
			app.DBName,
			app.IncludeTable[0],
			app.ColumnName,
			app.NewDbName,
			app.NewTableName,
			app.NewIndexName,
			app.EstimateTableRows,
			app.EstimateTableSize,
			app.RegionSize,
			app.Concurrency,
			app.OutDir); err != nil {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
		return fmt.Errorf("only support configre flag include, and only single table")
	}
	return nil
}

/*
	Base estimate split
*/

type AppSplitSampling struct {
	*AppSplit         // embedded parent command storage
	EstimateTableRows int
	BaseDbName        string
	BaseTableName     string
	BaseIndexName     string
	NewDbName         string
	NewTableName      string
	NewIndexName      string
	OutDir            string
}

func (app *AppSplit) AppSplitSampling() Cmder {
	return &AppSplitSampling{AppSplit: app}
}

func (app *AppSplitSampling) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "sampling",
		Short:        "Generate split region from the distinct value of base table index",
		Long:         `Generate split region from the distinct value of base table index`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVar(&app.EstimateTableRows, "new-table-row", 0, "estimate need be split table rows")
	cmd.Flags().StringVar(&app.BaseDbName, "base-db", "", "base estimate table db name")
	cmd.Flags().StringVar(&app.BaseTableName, "base-table", "", "base estimate table name")
	cmd.Flags().StringVar(&app.BaseIndexName, "base-index", "", "base estimate table index name")
	cmd.Flags().StringVar(&app.NewDbName, "new-db", "", "configure generate split table new db name through base estimate table column name")
	cmd.Flags().StringVar(&app.NewTableName, "new-table", "", "configure generate split table new table name through base estimate table column name")
	cmd.Flags().StringVar(&app.NewIndexName, "new-index", "", "configure generate split table index name through base estimate table column name")
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/split", "split sql file output dir")
	return cmd
}

func (app *AppSplitSampling) validateParameters() error {
	msg := "flag `%s` is requirement, can not null"
	if app.BaseDbName == "" {
		return fmt.Errorf(msg, "base-db")
	}
	if app.BaseTableName == "" {
		return fmt.Errorf(msg, "base-table")
	}
	if app.BaseIndexName == "" {
		return fmt.Errorf(msg, "base-index")
	}
	if app.NewDbName == "" {
		return fmt.Errorf(msg, "new-db")
	}
	if app.NewTableName == "" {
		return fmt.Errorf(msg, "new-table")
	}
	if app.NewIndexName == "" {
		return fmt.Errorf(msg, "new-index")
	}
	if app.EstimateTableRows == 0 {
		return fmt.Errorf(msg, "new-table-row")
	}
	return nil
}

func (app *AppSplitSampling) RunE(cmd *cobra.Command, args []string) error {
	err := app.validateParameters()
	if err != nil {
		return err
	}
	engine, err := db.NewMysqlDSN(app.User, app.Password, app.Host, app.Port, app.BaseDbName)
	if err != nil {
		return err
	}

	return split.GenerateSplitByBaseTable(engine,
		app.BaseDbName,
		app.BaseTableName,
		app.BaseIndexName,
		app.NewDbName,
		app.NewTableName,
		app.NewIndexName,
		app.OutDir,
		app.EstimateTableRows)
}

/*
	Base csv split
*/

type AppSplitCSV struct {
	*AppSplit         // embedded parent command storage
	Separator         string
	Delimiter         string
	Header            bool
	NotNull           bool
	Null              string
	TrimLastSep       bool
	BackslashEscape   bool
	EstimateTableSize int
	RegionSize        int
	ShardRowIDBits    int
	DataDir           string
	OutDir            string
}

func (app *AppSplit) AppSplitCSV() Cmder {
	return &AppSplitCSV{AppSplit: app}
}

func (app *AppSplitCSV) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "csv",
		Short:        "Split single and joint index region base csv data",
		Long:         `Split single and joint index region base csv data`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&app.Separator, "separator", ",", "the field separator must be a single ASCII character")
	cmd.Flags().StringVar(&app.Delimiter, "delimiter", string('"'), "quote delimiter, if delimiter is empty, all fields will be dereferenced")
	cmd.Flags().BoolVar(&app.Header, "header", true, "all CSV files contain header rows. If true, the first row will be used as the column name. If it is false, the first row has no special characteristics and is processed as a normal data row")
	cmd.Flags().BoolVar(&app.NotNull, "not-null", false, "whether the CSV contains NULL, if true, no column of the CSV file can be parsed as NULL")
	cmd.Flags().StringVar(&app.Null, "null", "\\N'", "if `not-null` is false (ie CSV can contain NULL), Fields with the following values ​​will be parsed as NULL.")
	cmd.Flags().BoolVar(&app.TrimLastSep, "trim-last-step", false, "treat the separator field as a terminator, whether to remove the line ending with the separator")
	cmd.Flags().BoolVar(&app.BackslashEscape, "backslash-escape", true, "whether to parse the backslash escape character in the field")
	cmd.Flags().IntVar(&app.EstimateTableSize, "new-table-size", 0, "estimate need be split table size(M)")
	cmd.Flags().IntVar(&app.RegionSize, "region-size", 96, "estimate need be split table region size(M)")
	cmd.Flags().IntVar(&app.ShardRowIDBits, "shard-rowid-bits", 6, "estimate need be split table character primary key tidb row id scatter")
	cmd.Flags().StringVar(&app.DataDir, "data-dir", "/tmp/split", "csv file store data dir")
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/split", "split sql file output dir")
	return cmd
}

func (app *AppSplitCSV) RunE(cmd *cobra.Command, args []string) error {
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

	//only support single table
	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "":
		if len(app.IncludeTable) != 1 {
			return fmt.Errorf(" flag include only support configre single table")
		}

	default:
		if err := cmd.Help(); err != nil {
			return err
		}
		return fmt.Errorf("only support configre flag include, and only single table")
	}
	return nil
}

/*
	Base key split
*/
type AppSplitKey struct {
	// embedded parent command storage
	*AppSplit
	TiDBStatusPort int
	OutDir         string
}

func (app *AppSplit) AppSplitKey() Cmder {
	return &AppSplitKey{AppSplit: app}
}

func (app *AppSplitKey) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "key",
		Short:        "Split region base region key",
		Long:         `Split region base region key`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVar(&app.TiDBStatusPort, "status-port", 10080, "tidb server status port")
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/split", "split sql file output dir")

	return cmd
}

func (app *AppSplitKey) RunE(cmd *cobra.Command, args []string) error {
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

	// get tidb server status port
	statusAddr := fmt.Sprintf("%s:%d", app.Host, app.TiDBStatusPort)
	if app.All {
		if err := split.AllTableSplitKey(app.DBName, statusAddr, app.Concurrency, app.OutDir, engine); err != nil {
			return err
		}
	}

	switch {
	case app.IncludeTable != nil && app.ExcludeTable == nil && app.RegexTable == "":
		if err := split.IncludeTableSplitKey(app.DBName, statusAddr, app.Concurrency, app.IncludeTable, app.OutDir, engine); err != nil {
			return err
		}
	case app.IncludeTable == nil && app.ExcludeTable != nil && app.RegexTable == "":
		if err := split.FilterTableSplitKey(app.DBName, statusAddr, app.Concurrency, app.IncludeTable, app.OutDir, engine); err != nil {
			return err
		}
	case app.IncludeTable == nil && app.ExcludeTable == nil && app.RegexTable != "":
		if err := split.RegexpTableSplitKey(app.DBName, statusAddr, app.Concurrency, app.RegexTable, app.OutDir, engine); err != nil {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
