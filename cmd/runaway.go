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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/model/runaway"
)

type AppRunaway struct {
	*App
}

func (a *App) AppRunaway() Cmder {
	return &AppRunaway{App: a}
}

func (a *AppRunaway) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runaway",
		Short: "RUNAWAY Used to fast SQL flow control based on resource control and problematic statement sql digest（require >= v8.5.0）",
		Long:  "RUNAWAY Used to fast SQL flow control based on resource control and problematic statement sql digest where the specified cluster name is located（require >= v8.5.0）",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
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
	return cmd
}

type AppRunawayCreate struct {
	*AppRunaway
	rgName    string
	ruPerSec  int
	priority  string
	sqlDigest string
	sqlText   string
	action    string
}

func (a *AppRunaway) AppRunawayCreate() Cmder {
	return &AppRunawayCreate{AppRunaway: a}
}

func (a *AppRunawayCreate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create Used to fast SQL flow control based on resource control and problematic statement sql digest（require >= v8.5.0）",
		Long:  "Create Used to fast SQL flow control based on resource control and problematic statement sql digest where the specified cluster name is located（require >= v8.5.0）",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}

			if a.sqlDigest == "" && a.sqlText == "" {
				return fmt.Errorf(`the sql_digest or sql_text cannot be empty, required flag(s) --sql-digest {sqlDigest} or --sql-text {sqlText} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(runaway.NewSqlRunawayModel(
				a.clusterName,
				a.sqlDigest, a.rgName, a.ruPerSec, a.priority, "CREATE", a.sqlText, a.action, nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(runaway.SqlRunawayModel)
			if lModel.Error != nil {
				return lModel.Error
			}

			if lModel.Msgs != nil {
				resp := lModel.Msgs.(*runaway.QueriedRespMsg)
				if reflect.DeepEqual(resp, &runaway.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql runaway not found, please ignore and skip")
					return nil
				}

				if len(resp.Results) > 0 {
					runaway.PrintSqlRunawayComment()
					fmt.Println("\ncluster topsql runaway query content:")
					if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
						return err
					}
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.rgName, "resource-group", "", "configure the cluster database operation resource group name, if not setting, then operation all of the database resource name")
	cmd.Flags().IntVar(&a.ruPerSec, "ru-per-sec", 5, "configure the cluster database resource group ru_per_sec parameters")
	cmd.Flags().StringVar(&a.priority, "priority", "low", "configure the cluster database resource group name priority")
	cmd.Flags().StringVar(&a.sqlDigest, "sql-digest", "", "configure the cluster database resource group with sql digest")
	cmd.Flags().StringVar(&a.sqlText, "sql-text", "", "configure the cluster database resource group with sql text")
	cmd.Flags().StringVar(&a.action, "action", "kill", "configure the cluster database resource group action, options: kill / switch")
	return cmd
}

type AppRunawayQuery struct {
	*AppRunaway
}

func (a *AppRunaway) AppRunawayQuery() Cmder {
	return &AppRunawayQuery{AppRunaway: a}
}

func (a *AppRunawayQuery) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query cluster runaway watch information records（require >= v8.5.0）",
		Long:  "Query cluster runaway watch information records where the specified cluster name is located（require >= v8.5.0）",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(runaway.NewSqlRunawayModel(
				a.clusterName,
				"",
				"",
				0,
				"",
				"QUERY",
				"",
				"",
				nil,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(runaway.SqlRunawayModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				resp := lModel.Msgs.(*runaway.QueriedRespMsg)
				if reflect.DeepEqual(resp, &runaway.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql runaway not found, please ignore and skip")
					return nil
				}

				if len(resp.Results) > 0 {
					fmt.Println("\ncluster topsql runaway query content:")
					if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
						return err
					}
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppRunawayDelete struct {
	*AppRunaway
	watchID []int
}

func (a *AppRunaway) AppRunawayDelete() Cmder {
	return &AppRunawayDelete{AppRunaway: a}
}

func (a *AppRunawayDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete the runaway query of the specified watch ids in the cluster（require >= v8.5.0）",
		Long:  "Delete the runaway query of the specified watch ids in the cluster where the specified cluster name is located（require >= v8.5.0）",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}

			if len(a.watchID) == 0 {
				return fmt.Errorf(`the watch_id cannot be empty, required flag(s) --watch-ids {watchID} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(runaway.NewSqlRunawayModel(
				a.clusterName,
				"",
				"",
				0,
				"",
				"DELETE",
				"",
				"",
				a.watchID,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(runaway.SqlRunawayModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				resp := lModel.Msgs.(*runaway.QueriedRespMsg)
				if reflect.DeepEqual(resp, &runaway.QueriedRespMsg{}) {
					fmt.Println("the cluster topsql runaway not found, please ignore and skip")
					return nil
				}

				if len(resp.Results) > 0 {
					fmt.Println("\ncluster topsql runaway query content:")
					if err := model.QueryResultFormatTableStyleWithRowsArray(resp.Columns, resp.Results); err != nil {
						return err
					}
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().IntSliceVar(&a.watchID, "watch-ids", nil, "configure the cluster database runaway watch ids")
	return cmd
}
