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
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/model"
)

type AppLogin struct {
	*App
}

func (a *App) AppLogin() Cmder {
	return &AppLogin{App: a}
}

func (a *AppLogin) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "login the cluster database",
		Long:  `Login to the cluster and database where the specified cluster name is located`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// remove metadata database
			clusters := database.Connector.GetNonMetadataClusters()
			activeClusterNums := len(clusters)

			if activeClusterNums > 1 {
				return fmt.Errorf("the current number of active clusters [%v] exceeds the limit, and only a single cluster database connection is allowed to survive at the same time", strings.Join(clusters, ","))
			}

			if activeClusterNums == 1 && !strings.EqualFold(clusters[0], a.clusterName) {
				return fmt.Errorf("the cluster_name [%v] are actived. multiple clusters cannot be run at the same time. please logout and login again", clusters[0])
			}
			if activeClusterNums == 1 && strings.EqualFold(clusters[0], a.clusterName) {
				return fmt.Errorf("the cluster_name [%v] are actived. no need to log in again", clusters[0])
			}

			p := tea.NewProgram(model.NewClusterLoginModel(a.clusterName))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			// the bubbletea component renders the View data too large, exceeding the terminal height or width, and the data terminal display is truncated. Use fmt.Printf directly to print and display
			// type assertion to get the final model state
			lModel := teaModel.(model.ClusterLoginModel)

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

			// reset app prompt
			if !a.disableInteractive {
				cli.SetClusterPrompt(a.clusterName)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
		Hidden:           true,
	}
	return cmd
}

type AppLogout struct {
	*App
}

func (a *App) AppLogout() Cmder {
	return &AppLogout{App: a}
}

func (a *AppLogout) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "logout the active cluster database",
		Long:  "Logout to the cluster database connection active in the current terminal",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			_, isLogin := database.Connector.LoadDatabase(a.clusterName)
			if !isLogin {
				return fmt.Errorf("the cluster_name [%s] database not login, no need logout", a.clusterName)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(model.NewClusterLogoutModel(a.clusterName))
			if _, err := p.Run(); err != nil {
				return err
			}
			// reset app prompt
			if !a.disableInteractive {
				cli.SetDefaultPrompt()
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
		Hidden:           true,
	}
	return cmd
}
