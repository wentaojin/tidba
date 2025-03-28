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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/utils/stringutil"
)

type AppMeta struct {
	*App
}

func (a *App) AppMeta() Cmder {
	return &AppMeta{App: a}
}

func (a *AppMeta) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "meta",
		Short: "metadata operation",
		Long:  "Options for cluster metadata information creation, deletion, update, query and other operations",
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

type AppMetaCreate struct {
	*AppMeta
}

func (a *AppMeta) AppMetaCreate() Cmder {
	return &AppMetaCreate{AppMeta: a}
}

func (a *AppMetaCreate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create the cluster metadata",
		Long:  "Create the configuration information required for cluster access",
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(model.NewClusterCreateModel())
			if _, err := p.Run(); err != nil {
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

type AppMetaDelete struct {
	*AppMeta
	force bool
}

func (a *AppMeta) AppMetaDelete() Cmder {
	return &AppMetaDelete{AppMeta: a}
}

func (a *AppMetaDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the cluster metadata",
		Long:  "Delete all configuration information of the specified cluster name",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if !a.disableInteractive {
				if !a.force {
					if err := stringutil.PromptForAnswerOrAbortError(
						"Yes, I know cluster config will be deleted.",
						"%s", fmt.Sprintf("This operation will destroy cluster %s config data.\nAre you sure to continue?", color.HiYellowString(a.clusterName)),
					); err != nil {
						return err
					}
				}
			}
			p := tea.NewProgram(model.NewClusterDeleteModel(a.clusterName))
			if _, err := p.Run(); err != nil {
				return err
			}

			// reset app prompt
			if !a.disableInteractive {
				if err := database.Connector.CloseDatabase(a.clusterName); err != nil {
					return err
				}
				cli.SetDefaultPrompt()
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().BoolVarP(&a.force, "delete", "f", false, "ignore deletion verification and force deletion")
	return cmd
}

type AppMetaUpdate struct {
	*AppMeta
}

func (a *AppMeta) AppMetaUpdate() Cmder {
	return &AppMetaUpdate{AppMeta: a}
}

func (a *AppMetaUpdate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update the cluster metadata",
		Long:  "Modify the configuration information of the specified cluster name",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(model.NewClusterUpdateModel(a.clusterName))
			if _, err := p.Run(); err != nil {
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

type AppMetaQuery struct {
	*AppMeta
}

func (a *AppMeta) AppMetaQuery() Cmder {
	return &AppMetaQuery{AppMeta: a}
}

func (a *AppMetaQuery) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "query the cluster metadata",
		Long:  "Query the cluster information of the specified cluster name",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(model.NewClusterListModel(a.clusterName, 0, 0))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			// the bubbletea component renders the View data too large, exceeding the terminal height or width, and the data terminal display is truncated. Use fmt.Printf directly to print and display
			// type assertion to get the final model state
			lModel := teaModel.(model.ClusterListModel)

			if lModel.Error == nil && len(lModel.Msgs) > 0 {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"cluster_name", "database", "path", "private_key"})
				t.AppendSeparator()
				for _, c := range lModel.Msgs {
					t.AppendRow(table.Row{
						c.ClusterName,
						fmt.Sprintf("%s[%s]@%s:%d", c.DbUser, c.DbPassword, c.DbHost, c.DbPort),
						c.Path,
						c.PrivateKey})
				}
				fmt.Printf("cluster config content:\n%s\n\n", t.Render())
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppMetaList struct {
	*AppMeta
	page     uint64
	pageSize uint64
}

func (a *AppMeta) AppMetaList() Cmder {
	return &AppMetaList{AppMeta: a}
}

func (a *AppMetaList) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the cluster metadata",
		Long:  "Get information about all accessible clusters",
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(model.NewClusterListModel("", a.page, a.pageSize))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			// the bubbletea component renders the View data too large, exceeding the terminal height or width, and the data terminal display is truncated. Use fmt.Printf directly to print and display
			// type assertion to get the final model state
			lModel := teaModel.(model.ClusterListModel)

			if lModel.Error == nil && len(lModel.Msgs) > 0 {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"cluster_name", "database", "path", "private_key"})
				t.AppendSeparator()
				for _, c := range lModel.Msgs {
					t.AppendRow(table.Row{
						c.ClusterName,
						fmt.Sprintf("%s[%s]@%s:%d", c.DbUser, c.DbPassword, c.DbHost, c.DbPort),
						c.Path,
						c.PrivateKey})
				}
				fmt.Printf("cluster config content:\n%s\n\n", t.Render())
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().Uint64Var(&a.page, "page", 1, "specify the query results page")
	cmd.Flags().Uint64Var(&a.pageSize, "pageSize", 50, "specify the query results page size")
	return cmd
}
