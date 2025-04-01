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
	"reflect"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/model/topsql"
	"github.com/wentaojin/tidba/utils/cluster/operator"
)

type AppTopsql struct {
	*App

	nearly    int
	startTime string
	endTime   string
	top       int
	enableSql bool
}

func (a *App) AppTopsql() Cmder {
	return &AppTopsql{App: a}
}

func (a *AppTopsql) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topsql",
		Short: "Topsql display the SQL statements for cluster database resource consumption",
		Long:  "Topsql display the SQL statements for cluster database resource consumption where the specified cluster name is located",
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

	cmd.PersistentFlags().IntVar(&a.nearly, "nearly", 30, "configure the cluster database query time windows, size: minutes")
	cmd.PersistentFlags().StringVar(&a.startTime, "start", "", "configure the cluster database query range with start time")
	cmd.PersistentFlags().StringVar(&a.endTime, "end", "", "configure the cluster database query range with end time")
	cmd.PersistentFlags().IntVar(&a.top, "top", 10, "configure the cluster database query top sql")
	cmd.PersistentFlags().BoolVar(&a.enableSql, "enable-sql", false, "configure the cluster database query result display sql_text if setting enable_sql")
	return cmd
}

type AppTopsqlElapsed struct {
	*AppTopsql
	enableHistory bool
}

func (a *AppTopsql) AppTopsqlElapsed() Cmder {
	return &AppTopsqlElapsed{AppTopsql: a}
}

func (a *AppTopsqlElapsed) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "elapsed",
		Short: "Elapsed display the TOPSQL statements for cluster database sql elapsed time",
		Long:  "Elapsed display the TOPSQL statements for cluster database sql elapsed time where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(topsql.NewTopsqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				a.top,
				"ELAPSED",
				0,
				"",
				a.enableSql,
				nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(topsql.TopsqlQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*topsql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &topsql.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql not found, please ignore and skip")
					return nil
				}

				topsql.PrintTopsqlElapsedTimeComment(a.top)
				fmt.Println("\ncluster topsql query content:")
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
					return err
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().BoolVar(&a.enableHistory, "enable-history", false, "configure the cluster database query system cluster_statements_summary if enable history")
	return cmd
}

type AppTopsqlExecutions struct {
	*AppTopsql
	enableHistory bool
}

func (a *AppTopsql) AppTopsqlExecutions() Cmder {
	return &AppTopsqlExecutions{AppTopsql: a}
}

func (a *AppTopsqlExecutions) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "execs",
		Short: "Execs display the TOPSQL statements for cluster database sql executions",
		Long:  "Execs display the TOPSQL statements for cluster database sql executions where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(topsql.NewTopsqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				a.top,
				"EXECUTIONS",
				0,
				"",
				a.enableSql,
				nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(topsql.TopsqlQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*topsql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &topsql.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql not found, please ignore and skip")
					return nil
				}
				topsql.PrintTopsqlExecutionsComment(a.top)
				fmt.Println("\ncluster topsql query content:")
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
					return err
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().BoolVar(&a.enableHistory, "enable-history", false, "configure the cluster database query system cluster_statements_summary if enable history")
	return cmd
}

type AppTopsqlPlans struct {
	*AppTopsql
	enableHistory bool
}

func (a *AppTopsql) AppTopsqlPlans() Cmder {
	return &AppTopsqlPlans{AppTopsql: a}
}

func (a *AppTopsqlPlans) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plans",
		Short: "Plans display the TOPSQL statements for cluster database sql plan counts",
		Long:  "Plans display the TOPSQL statements for cluster database sql plan counts where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(topsql.NewTopsqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				a.top,
				"PLANS",
				0,
				"",
				a.enableSql,
				nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(topsql.TopsqlQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*topsql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &topsql.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql not found, please ignore and skip")
					return nil
				}

				topsql.PrintTopsqlPlansComment(a.top)
				fmt.Println("\ncluster topsql query content:")
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
					return err
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().BoolVar(&a.enableHistory, "enable-history", false, "configure the cluster database query system cluster_statements_summary if enable history")
	return cmd
}

type AppTopsqlCPU struct {
	*AppTopsql
	component   string
	instances   []string
	concurrency int
}

func (a *AppTopsql) AppTopsqlCPU() Cmder {
	return &AppTopsqlCPU{AppTopsql: a}
}

func (a *AppTopsqlCPU) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cpu",
		Short: "CPU display the TOPSQL statements for cluster database sql tidb cpu time or tikv cpu time",
		Long:  "CPU display the TOPSQL statements for cluster database sql tidb cpu time or tikv cpu time where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if !strings.EqualFold(a.component, operator.ComponentNameTiDB) && !strings.EqualFold(a.component, operator.ComponentNameTiKV) {
				return fmt.Errorf("unknown component [%s], only support options for tidb / tikv", a.component)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(topsql.NewTopsqlQueryModel(
				a.clusterName,
				a.nearly,
				false,
				a.startTime,
				a.endTime,
				a.top,
				"CPU",
				a.concurrency,
				a.component,
				a.enableSql,
				a.instances,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(topsql.TopsqlQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*topsql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &topsql.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql not found, please ignore and skip")
					return nil
				}

				if strings.EqualFold(a.component, operator.ComponentNameTiDB) {
					topsql.PrintTopsqlCpuByTidbComment(a.top)
				}
				if strings.EqualFold(a.component, operator.ComponentNameTiKV) {
					topsql.PrintTopsqlCpuByTikvComment(a.top)
				}
				fmt.Println("\ncluster topsql query content:")
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
					return err
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.component, "component", "tidb", "configure the cluster database query component cpu time, options: tidb / tikv")
	cmd.Flags().StringSliceVar(&a.instances, "instances", nil, "configure the cluster database query component instances cpu time, need configure {instAddr:statusPort}")
	cmd.Flags().IntVar(&a.concurrency, "concurrency", 5, "configure the cluster database query component cpu time concurrency")

	return cmd
}

type AppTopsqlDiagnosis struct {
	*AppTopsql
	concurrency   int
	enableHistory bool
}

func (a *AppTopsql) AppTopsqlDiagnosis() Cmder {
	return &AppTopsqlDiagnosis{AppTopsql: a}
}

func (a *AppTopsqlDiagnosis) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diag",
		Short: "Diagnosis display the top 5 SQL statements that affect cluster performance",
		Long:  "Diagnosis display the top 5 SQL statements that affect cluster performance where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(topsql.NewTopsqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				a.top,
				"DIAGNOSIS",
				a.concurrency,
				"",
				a.enableSql,
				nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(topsql.TopsqlQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*topsql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &topsql.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql not found, please ignore and skip")
					return nil
				}

				topsql.PrintTopsqlDiagnosisComment()
				fmt.Println("\ncluster topsql query content:")
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
					return err
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().IntVar(&a.concurrency, "concurrency", 5, "configure the cluster database query component cpu time concurrency")
	cmd.Flags().BoolVar(&a.enableHistory, "enable-history", false, "configure the cluster database query system cluster_statements_summary if enable history")
	return cmd
}
