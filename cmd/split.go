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
	"context"
	"fmt"
	"io"
	"log"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/model/split"
)

type AppSplit struct {
	*App

	database    string
	tables      []string
	concurrency int
	output      string
	daemon      bool
}

func (a *App) AppSplit() Cmder {
	return &AppSplit{App: a}
}

func (a *AppSplit) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "split",
		Short: "split the cluster database table region",
		Long:  "Split to the cluster database table region where the specified cluster name is located",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return err
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.PersistentFlags().StringVar(&a.database, "database", "", "configure the cluster database name")
	cmd.PersistentFlags().StringSliceVar(&a.tables, "tables", nil, "configure the cluster database table names")
	cmd.PersistentFlags().IntVar(&a.concurrency, "concurrency", 5, "configure the cluster database table split task concurrency")
	cmd.PersistentFlags().StringVarP(&a.output, "output", "O", "/tmp", "configure the cluster database table split output dir")
	cmd.PersistentFlags().BoolVar(&a.daemon, "daemon", false, "configure the cluster database table split run as a daemon")
	cmd.MarkPersistentFlagRequired("database")
	cmd.MarkPersistentFlagRequired("tables")
	return cmd
}

type AppSplitRange struct {
	*AppSplit
}

func (a *AppSplit) AppSplitRange() Cmder {
	return &AppSplitRange{AppSplit: a}
}

func (a *AppSplitRange) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "range",
		Short: "Generate split data and index statements based on database table range",
		Long:  "Generate split data and index statements based on database table range",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []tea.ProgramOption
			if a.daemon || !isatty.IsTerminal(os.Stdout.Fd()) {
				// If we're in daemon mode don't render the TUI
				opts = []tea.ProgramOption{tea.WithoutRenderer()}
			} else {
				// If we're in TUI mode, discard log output
				log.SetOutput(io.Discard)
			}

			p := tea.NewProgram(
				split.NewTableSplitModel(a.output),
				opts...)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				if err := split.RunTableSplitTask(
					ctx,
					a.clusterName,
					&split.SplitReqMsg{
						Database:    a.database,
						Tables:      a.tables,
						Output:      a.output,
						Concurrency: a.concurrency,
						Command:     "RANGE",
					}); err != nil {
					cancel()
					p.Send(split.SplitRespMsg{Err: err})
					return
				}
				p.Send(split.SplitRespMsg{Err: nil})
			}()

			teaModel, err := p.Run()
			if err != nil {
				return err
			}

			lModel := teaModel.(split.TableSplitModel)
			if lModel.Err != nil {
				fmt.Println(lModel.Err)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppSplitKey struct {
	*AppSplit
}

func (a *AppSplit) AppSplitKey() Cmder {
	return &AppSplitKey{AppSplit: a}
}

func (a *AppSplitKey) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "key",
		Short: "Generate split data and index statements based on database table region start_key",
		Long:  "Generate split data and index statements based on database table region start_key",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []tea.ProgramOption
			if a.daemon || !isatty.IsTerminal(os.Stdout.Fd()) {
				// If we're in daemon mode don't render the TUI
				opts = []tea.ProgramOption{tea.WithoutRenderer()}
			} else {
				// If we're in TUI mode, discard log output
				log.SetOutput(io.Discard)
			}

			p := tea.NewProgram(
				split.NewTableSplitModel(a.output),
				opts...)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				if err := split.RunTableSplitTask(
					ctx,
					a.clusterName,
					&split.SplitReqMsg{
						Database:    a.database,
						Tables:      a.tables,
						Output:      a.output,
						Concurrency: a.concurrency,
						Command:     "KEY",
					}); err != nil {
					cancel()
					p.Send(split.SplitRespMsg{Err: err})
					return
				}
				p.Send(split.SplitRespMsg{Err: nil})
			}()

			teaModel, err := p.Run()
			if err != nil {
				return err
			}

			lModel := teaModel.(split.TableSplitModel)
			if lModel.Err != nil {
				fmt.Println(lModel.Err)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppSplitSampling struct {
	*AppSplit

	indexName    string
	newDbName    string
	newTableName string
	newIndexName string
	estimateRows int
	resource     string
}

func (a *AppSplit) AppSplitSampling() Cmder {
	return &AppSplitSampling{AppSplit: a}
}

func (a *AppSplitSampling) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sampling",
		Short: "Generate split index statement based on the specified sampling index of the database table",
		Long:  "Generate split index statement based on the specified sampling index of the database table",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if len(a.tables) > 1 {
				return fmt.Errorf(`the sampling command only used for a certain table or index in a certain database. flag --tables cannot configure multiple tables`)
			}
			if a.newTableName == "" && len(a.tables) == 1 {
				a.newTableName = a.tables[0]
			}
			if a.estimateRows == 0 {
				return fmt.Errorf("the sampling command flag [--estimate-row] cannot be zero, please configure and retry")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []tea.ProgramOption
			if a.daemon || !isatty.IsTerminal(os.Stdout.Fd()) {
				// If we're in daemon mode don't render the TUI
				opts = []tea.ProgramOption{tea.WithoutRenderer()}
			} else {
				// If we're in TUI mode, discard log output
				log.SetOutput(io.Discard)
			}

			p := tea.NewProgram(
				split.NewTableSplitModel(a.output),
				opts...)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				if err := split.RunTableSplitTask(
					ctx,
					a.clusterName,
					&split.SplitReqMsg{
						Database:     a.database,
						Tables:       a.tables,
						Output:       a.output,
						Concurrency:  a.concurrency,
						IndexName:    a.indexName,
						NewDbName:    a.newDbName,
						NewTableName: a.newTableName,
						NewIndexName: a.newIndexName,
						Resource:     a.resource,
						EstimateRows: a.estimateRows,
						Command:      "SAMPLING",
					}); err != nil {
					cancel()
					p.Send(split.SplitRespMsg{Err: err})
					return
				}
				p.Send(split.SplitRespMsg{Err: nil})
			}()

			teaModel, err := p.Run()
			if err != nil {
				return err
			}

			lModel := teaModel.(split.TableSplitModel)
			if lModel.Err != nil {
				fmt.Println(lModel.Err)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().StringVar(&a.indexName, "index", "", "configure the cluster database table index name")
	cmd.Flags().StringVar(&a.newDbName, "new-db", a.database, "configure the generate split index statement database name, the default is the same as the original database name")
	cmd.Flags().StringVar(&a.newTableName, "new-table", "", "configure the generate split index statement table name, the default is the same as the original table name")
	cmd.Flags().StringVar(&a.newIndexName, "new-index", a.indexName, "configure the generate split index statement index name, the default is the same as the original index name")
	cmd.Flags().StringVar(&a.resource, "resource", "server", "configure split resource consumption mode, optional: server or database")
	cmd.Flags().IntVar(&a.estimateRows, "estimate-row", 0, "configure the generate split index statement estimate write rows, cannot be zero")
	cmd.MarkFlagRequired("index")
	cmd.MarkFlagRequired("estimate-row")
	return cmd
}

type AppSplitEstimate struct {
	*AppSplit

	columnNames  []string
	newDbName    string
	newTableName string
	newIndexName string
	estimateRows int
	estimateSize int
	regionSize   int
}

func (a *AppSplit) AppSplitEstimate() Cmder {
	return &AppSplitEstimate{AppSplit: a}
}

func (a *AppSplitEstimate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "estimate",
		Short: "Generate split index statement based on the specified columns of the database table",
		Long:  "Generate split index statement based on the specified columns of the database table",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if len(a.tables) > 1 {
				return fmt.Errorf(`the sampling command only used for a certain table or index in a certain database. flag --tables cannot configure multiple tables`)
			}
			if a.newTableName == "" && len(a.tables) == 1 {
				a.newTableName = a.tables[0]
			}
			if a.estimateRows == 0 {
				return fmt.Errorf("the sampling command flag [--estimate-row] cannot be zero, please configure and retry")
			}
			if a.estimateSize == 0 {
				return fmt.Errorf("the sampling command flag [--estimate-size] cannot be zero, please configure and retry")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []tea.ProgramOption
			if a.daemon || !isatty.IsTerminal(os.Stdout.Fd()) {
				// If we're in daemon mode don't render the TUI
				opts = []tea.ProgramOption{tea.WithoutRenderer()}
			} else {
				// If we're in TUI mode, discard log output
				log.SetOutput(io.Discard)
			}

			p := tea.NewProgram(
				split.NewTableSplitModel(a.output),
				opts...)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				if err := split.RunTableSplitTask(
					ctx,
					a.clusterName,
					&split.SplitReqMsg{
						Database:     a.database,
						Tables:       a.tables,
						Output:       a.output,
						Concurrency:  a.concurrency,
						ColumnNames:  a.columnNames,
						NewDbName:    a.newDbName,
						NewTableName: a.newTableName,
						NewIndexName: a.newIndexName,
						EstimateRows: a.estimateRows,
						EstimateSize: a.estimateSize,
						RegionSize:   a.regionSize,
						Command:      "ESTIMATE",
					}); err != nil {
					cancel()
					p.Send(split.SplitRespMsg{Err: err})
					return
				}
				p.Send(split.SplitRespMsg{Err: nil})
			}()

			teaModel, err := p.Run()
			if err != nil {
				return err
			}

			lModel := teaModel.(split.TableSplitModel)
			if lModel.Err != nil {
				fmt.Println(lModel.Err)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().StringArrayVar(&a.columnNames, "columns", nil, "configure the cluster database table column names")
	cmd.Flags().StringVar(&a.newDbName, "new-db", a.database, "configure the generate split index statement database name, the default is the same as the original database name")
	cmd.Flags().StringVar(&a.newTableName, "new-table", "", "configure the generate split index statement table name, the default is the same as the original table name")
	cmd.Flags().StringVar(&a.newIndexName, "new-index", "", "configure the generate split index statement index name, the default is the same as the original index name")
	cmd.Flags().IntVar(&a.estimateRows, "estimate-row", 0, "configure the generate split index statement estimate write rows, cannot be zero")
	cmd.Flags().IntVar(&a.estimateSize, "estimate-size", 0, "configure the generate split index statement estimate write size（size: MB）, cannot be zero")
	cmd.Flags().IntVar(&a.regionSize, "region-size", 96, "configure a single region size to estimate how many regions to generate（size: MB）")
	cmd.MarkFlagRequired("columns")
	cmd.MarkFlagRequired("estimate-row")
	cmd.MarkFlagRequired("estimate-size")
	cmd.MarkFlagRequired("new-index")
	return cmd
}
