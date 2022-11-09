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
	split2 "github.com/wentaojin/tidba/split"

	"github.com/spf13/cobra"
)

// AppSplit is storage for the sub command analyze
// includeTable、excludeTables、regexTables only one of the three
type AppSplit struct {
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
	outDir        string
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

	cmd.PersistentFlags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.dbName, "db", "D", "", "database name")
	cmd.PersistentFlags().StringSliceVarP(&app.includeTables, "include", "i", nil, "configure table name")
	cmd.PersistentFlags().StringSliceVarP(&app.excludeTables, "exclude", "e", nil, "configure exclude table name")
	cmd.PersistentFlags().StringVarP(&app.regexTables, "regex", "r", "", "configure table name by go regexp")
	cmd.PersistentFlags().BoolVar(&app.all, "all", false, "all tables in the database")
	cmd.PersistentFlags().StringVarP(&app.outDir, "out-dir", "o", "/tmp/split", "gen split file output dir")
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
}

func (app *AppSplit) AppSplitRange() Cmder {
	return &AppSplitRange{AppSplit: app}
}

func (app *AppSplitRange) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "range",
		Short:        "Generate split region from the range of table - exist",
		Long:         `Generate split region from the range of table - exist`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (app *AppSplitRange) RunE(cmd *cobra.Command, args []string) error {
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
		if err := split2.AllTableSplitRange(app.dbName, app.Concurrency, app.outDir, engine); err != nil {
			return err
		}
	}

	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "":
		if err := split2.IncludeTableSplitRange(app.dbName, app.Concurrency, app.includeTables, app.outDir, engine); err != nil {
			return err
		}
	case app.includeTables == nil && app.excludeTables != nil && app.regexTables == "":
		if err := split2.FilterTableSplitRange(app.dbName, app.Concurrency, app.includeTables, app.outDir, engine); err != nil {
			return err
		}
	case app.includeTables == nil && app.excludeTables == nil && app.regexTables != "":
		if err := split2.RegexpTableSplitRange(app.dbName, app.Concurrency, app.regexTables, app.outDir, engine); err != nil {
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
	*AppSplit          // embedded parent command storage
	estimateTableRows  int
	estimateTableSize  int
	estimateColumnName string
	regionSize         int
	genDbName          string
	genTableName       string
	genIndexName       string
}

func (app *AppSplit) AppSplitEstimate() Cmder {
	return &AppSplitEstimate{AppSplit: app}
}

func (app *AppSplitEstimate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "estimate",
		Short:        "Generate split region from the distinct factor value of base table index - expensive",
		Long:         `Generate split region from the distinct factor value of base table index - expensive`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVar(&app.estimateTableRows, "estimate-row", 0, "estimate need be split table rows")
	cmd.Flags().IntVar(&app.estimateTableSize, "estimate-size", 0, "estimate need be split table size(M)")
	cmd.Flags().StringVar(&app.estimateColumnName, "estimate-column", "", "configure sample base estimate table column name")
	cmd.Flags().IntVar(&app.regionSize, "region-size", 96, "estimate need be split table region size(M)")
	cmd.Flags().StringVar(&app.genDbName, "gen-db", "", "configure generate split table new db name through base estimate table column name")
	cmd.Flags().StringVar(&app.genTableName, "gen-table", "", "configure generate split table new table name through base estimate table column name")
	cmd.Flags().StringVar(&app.genIndexName, "gen-index", "", "configure generate split table index name through base estimate table column name")

	return cmd
}

func (app *AppSplitEstimate) RunE(cmd *cobra.Command, args []string) error {
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

	//only support single table
	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "":
		if len(app.includeTables) != 1 {
			return fmt.Errorf(" flag include only support configre single table")
		}
		if app.genIndexName == "" {
			return fmt.Errorf("flag new index name is requirement, can not null")

		}
		if err := split2.IncludeTableSplitEstimate(engine,
			app.dbName,
			app.includeTables[0],
			app.estimateColumnName,
			app.genDbName,
			app.genTableName,
			app.genIndexName,
			app.estimateTableRows,
			app.estimateTableSize,
			app.regionSize,
			app.Concurrency,
			app.outDir); err != nil {
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
	Base sampling split
*/

type AppSplitSampling struct {
	*AppSplit         // embedded parent command storage
	estimateTableRows int
	baseDbName        string
	baseTableName     string
	baseIndexName     string
	genDbName         string
	genTableName      string
	genIndexName      string
}

func (app *AppSplit) AppSplitSampling() Cmder {
	return &AppSplitSampling{AppSplit: app}
}

func (app *AppSplitSampling) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "sampling",
		Short:        "Generate split region from the distinct value of base table index - non-pagination",
		Long:         `Generate split region from the distinct value of base table index - non-pagination`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVar(&app.estimateTableRows, "estimate-row", 0, "estimate need be split table rows")
	cmd.Flags().StringVar(&app.baseDbName, "base-db", "", "base estimate table db name")
	cmd.Flags().StringVar(&app.baseTableName, "base-table", "", "base estimate table name")
	cmd.Flags().StringVar(&app.baseIndexName, "base-index", "", "base estimate table index name")
	cmd.Flags().StringVar(&app.genDbName, "gen-db", "", "configure generate split table new db name through base estimate table column name")
	cmd.Flags().StringVar(&app.genTableName, "gen-table", "", "configure generate split table new table name through base estimate table column name")
	cmd.Flags().StringVar(&app.genIndexName, "gen-index", "", "configure generate split table index name through base estimate table column name")
	cmd.Flags().StringVarP(&app.outDir, "out-dir", "o", "/tmp/split", "split sql file output dir")
	return cmd
}

func (app *AppSplitSampling) validateParameters() error {
	msg := "flag `%s` is requirement, can not null"
	if app.baseDbName == "" {
		return fmt.Errorf(msg, "base-db")
	}
	if app.baseTableName == "" {
		return fmt.Errorf(msg, "base-table")
	}
	if app.baseIndexName == "" {
		return fmt.Errorf(msg, "base-index")
	}
	if app.genDbName == "" {
		return fmt.Errorf(msg, "gen-db")
	}
	if app.genTableName == "" {
		return fmt.Errorf(msg, "gen-table")
	}
	if app.genIndexName == "" {
		return fmt.Errorf(msg, "gen-index")
	}
	if app.estimateTableRows == 0 {
		return fmt.Errorf(msg, "estimate-row")
	}
	return nil
}

func (app *AppSplitSampling) RunE(cmd *cobra.Command, args []string) error {
	err := app.validateParameters()
	if err != nil {
		return err
	}
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.baseDbName)
	if err != nil {
		return err
	}

	return split2.GenerateSplitByBaseTable(engine,
		app.baseDbName,
		app.baseTableName,
		app.baseIndexName,
		app.genDbName,
		app.genTableName,
		app.genIndexName,
		app.outDir,
		app.estimateTableRows)
}

/*
	Base reckon split
*/

type AppSplitReckon struct {
	*AppSplit         // embedded parent command storage
	estimateTableRows int
	baseDbName        string
	baseTableName     string
	baseIndexName     string
	genDbName         string
	genTableName      string
	genIndexName      string
}

func (app *AppSplit) AppSplitReckon() Cmder {
	return &AppSplitReckon{AppSplit: app}
}

func (app *AppSplitReckon) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "reckon",
		Short:        "Generate split region from the distinct value of base table index - pagination",
		Long:         `Generate split region from the distinct value of base table index - pagination`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVar(&app.estimateTableRows, "estimate-row", 0, "estimate need be split table rows")
	cmd.Flags().StringVar(&app.baseDbName, "base-db", "", "base estimate table db name")
	cmd.Flags().StringVar(&app.baseTableName, "base-table", "", "base estimate table name")
	cmd.Flags().StringVar(&app.baseIndexName, "base-index", "", "base estimate table index name")
	cmd.Flags().StringVar(&app.genDbName, "gen-db", "", "configure generate split table new db name through base estimate table column name")
	cmd.Flags().StringVar(&app.genTableName, "gen-table", "", "configure generate split table new table name through base estimate table column name")
	cmd.Flags().StringVar(&app.genIndexName, "gen-index", "", "configure generate split table index name through base estimate table column name")
	cmd.Flags().StringVarP(&app.outDir, "out-dir", "o", "/tmp/split", "split sql file output dir")
	return cmd
}

func (app *AppSplitReckon) validateParameters() error {
	msg := "flag `%s` is requirement, can not null"
	if app.baseDbName == "" {
		return fmt.Errorf(msg, "base-db")
	}
	if app.baseTableName == "" {
		return fmt.Errorf(msg, "base-table")
	}
	if app.baseIndexName == "" {
		return fmt.Errorf(msg, "base-index")
	}
	if app.genDbName == "" {
		return fmt.Errorf(msg, "gen-db")
	}
	if app.genTableName == "" {
		return fmt.Errorf(msg, "gen-table")
	}
	if app.genIndexName == "" {
		return fmt.Errorf(msg, "gen-index")
	}
	if app.estimateTableRows == 0 {
		return fmt.Errorf(msg, "estimate-row")
	}
	return nil
}

func (app *AppSplitReckon) RunE(cmd *cobra.Command, args []string) error {
	err := app.validateParameters()
	if err != nil {
		return err
	}
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.baseDbName)
	if err != nil {
		return err
	}

	return split2.GenerateSplitByReckonBaseTable(engine,
		app.baseDbName,
		app.baseTableName,
		app.baseIndexName,
		app.genDbName,
		app.genTableName,
		app.genIndexName,
		app.outDir,
		app.estimateTableRows)
}

/*
Base key split
*/
type AppSplitKey struct {
	// embedded parent command storage
	*AppSplit
	tidbStatusPort int
}

func (app *AppSplit) AppSplitKey() Cmder {
	return &AppSplitKey{AppSplit: app}
}

func (app *AppSplitKey) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "key",
		Short:        "Generate split region from the key of table - exist",
		Long:         `Generate split region from the key of table - exist`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVar(&app.tidbStatusPort, "status-port", 10080, "tidb server status port")
	cmd.Flags().StringVarP(&app.outDir, "out-dir", "o", "/tmp/split", "split sql file output dir")

	return cmd
}

func (app *AppSplitKey) RunE(cmd *cobra.Command, args []string) error {
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

	// get tidb server status port
	statusAddr := fmt.Sprintf("%s:%d", app.host, app.tidbStatusPort)
	if app.all {
		if err := split2.AllTableSplitKey(app.dbName, statusAddr, app.Concurrency, app.outDir, engine); err != nil {
			return err
		}
	}

	switch {
	case app.includeTables != nil && app.excludeTables == nil && app.regexTables == "":
		if err := split2.IncludeTableSplitKey(app.dbName, statusAddr, app.Concurrency, app.includeTables, app.outDir, engine); err != nil {
			return err
		}
	case app.includeTables == nil && app.excludeTables != nil && app.regexTables == "":
		if err := split2.FilterTableSplitKey(app.dbName, statusAddr, app.Concurrency, app.includeTables, app.outDir, engine); err != nil {
			return err
		}
	case app.includeTables == nil && app.excludeTables == nil && app.regexTables != "":
		if err := split2.RegexpTableSplitKey(app.dbName, statusAddr, app.Concurrency, app.regexTables, app.outDir, engine); err != nil {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
