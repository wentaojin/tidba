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
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/model/inspect"
	"github.com/wentaojin/tidba/utils/cluster/executor"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"github.com/wentaojin/tidba/utils/cluster/printer"
	"github.com/wentaojin/tidba/utils/stringutil"
)

type AppInspect struct {
	*App
}

func (a *App) AppInspect() Cmder {
	return &AppInspect{App: a}
}

func (a *AppInspect) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "inspect the cluster database",
		Long:  "Inspect to the cluster and database where the specified cluster name is located",
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

type AppClusterInspectCreate struct {
	*AppInspect
}

func (a *AppInspect) AppClusterInspectCreate() Cmder {
	return &AppClusterInspectCreate{AppInspect: a}
}

func (a *AppClusterInspectCreate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create the cluster inspction config",
		Long:  "Create config for the cluster and database where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(inspect.NewInspectCreateModel(a.clusterName))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}

			// the bubbletea component renders the View data too large, exceeding the terminal height or width, and the data terminal display is truncated. Use fmt.Printf directly to print and display
			// type assertion to get the final model state
			lModel := teaModel.(inspect.InspectCreateModel)

			if lModel.Error == nil && lModel.Msg != "" {
				fmt.Printf("cluster ispection config content:\n%s", lModel.Msg)
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppClusterInspectDelete struct {
	*AppInspect
	force bool
}

func (a *AppInspect) AppClusterInspectDelete() Cmder {
	return &AppClusterInspectDelete{AppInspect: a}
}

func (a *AppClusterInspectDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete the cluster inspction config",
		Long:  "Delete config for the cluster and database where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if !a.disableInteractive {
				if !a.force {
					if err := stringutil.PromptForAnswerOrAbortError(
						"Yes, I know cluster inspect config will be deleted.",
						"%s", fmt.Sprintf("This operation will destroy cluster %s inspect config data.\nAre you sure to continue?", color.HiYellowString(a.clusterName)),
					); err != nil {
						return err
					}
				}
			}
			p := tea.NewProgram(inspect.NewInspectDeleteModel(a.clusterName))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(inspect.InspectDeleteModel)

			if lModel.Error != nil {
				return lModel.Error
			}

			if lModel.Msg != "" {
				fmt.Printf("cluster ispection config content:\n%s", lModel.Msg)
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

type AppClusterInspectUpdate struct {
	*AppInspect
}

func (a *AppInspect) AppClusterInspectUpdate() Cmder {
	return &AppClusterInspectUpdate{AppInspect: a}
}

func (a *AppClusterInspectUpdate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update the cluster inspction config",
		Long:  "Update config for the cluster and database where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(inspect.NewInspectUpdateModel(a.clusterName))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(inspect.InspectUpdateModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msg != nil {
				fmt.Printf("cluster ispection config content:\n%s", lModel.Msg.String())
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppClusterInspectQuery struct {
	*AppInspect
}

func (a *AppInspect) AppClusterInspectQuery() Cmder {
	return &AppClusterInspectQuery{AppInspect: a}
}

func (a *AppClusterInspectQuery) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "query the cluster inspction config",
		Long:  "Query config for the cluster and database where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(inspect.NewInspectQueryModel(a.clusterName))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(inspect.InspectQueryModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msg != nil {
				fmt.Printf("cluster ispection config content:\n%s", lModel.Msg.String())
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppClusterInspectStart struct {
	*AppInspect
	sshUser      string
	usePassword  bool
	identityFile string
	sshTimeout   uint64
	waitTimeout  uint64
	ssh          string
	concurrency  int
	output       string
}

func (a *AppInspect) AppClusterInspectStart() Cmder {
	return &AppClusterInspectStart{AppInspect: a}
}

func (a *AppClusterInspectStart) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start the cluster inspction",
		Long:  "Start inspection for the cluster and database where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				l             *printer.Logger
				gOpt          *operator.Options
				err           error
				sshConnProps  *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
				sshProxyProps *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
			)

			if a.sshUser == "" {
				cuser, err := user.Current()
				if err != nil {
					return fmt.Errorf("can not get current username: [%v]", err)
				}
				a.sshUser = cuser.Username
			}

			// ~ home dir replace
			if a.identityFile != "" && strings.HasPrefix(a.identityFile, "~") {
				dir, err := homedir.Expand("~")
				if err != nil {
					return err
				}
				idenfSli := strings.Split(a.identityFile, "/")
				a.identityFile = filepath.Join(dir, strings.Join(idenfSli[1:], "/"))
			}

			l = printer.NewLogger("inspector")
			gOpt = &operator.Options{
				SSHUser:     a.sshUser,
				SSHPort:     22,
				SSHTimeout:  a.sshTimeout,
				OptTimeout:  a.waitTimeout,
				SSHType:     executor.SSHType(a.ssh),
				Concurrency: a.concurrency,
			}

			if gOpt.SSHType != executor.SSHTypeNone {
				sshConnProps, err = operator.ReadIdentityFileOrPassword(a.identityFile, a.usePassword)
				if err != nil {
					return err
				}
				if len(gOpt.SSHProxyHost) != 0 {
					if sshProxyProps, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
						return err
					}
				}
			}
			l.Infof("+ Start inspect %v cluster", color.RedString("[%v]", a.clusterName))

			insp, err := inspect.StartClusterInspect(
				context.Background(),
				a.clusterName,
				l,
				sshConnProps,
				sshProxyProps,
				gOpt)
			if err != nil {
				return err
			}

			currentTime := time.Now().Format("20060102150405")
			fileName := filepath.Join(a.output, fmt.Sprintf("insp_%s_report_%s.html", a.clusterName, currentTime))
			file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			defer file.Close()
			if err := inspect.GenClusterInspectReport(insp, file); err != nil {
				return err
			}

			abnormalFile := filepath.Join(a.output, fmt.Sprintf("insp_%s_dev_stats_abnormal_%s.xlsx", a.clusterName, currentTime))
			isAbnormal, err := inspect.GenClusterDevAndStatsAbnormalOutputExcel(
				abnormalFile,
				insp.DevAbnormals,
				insp.StatsAbnormals)

			if err != nil {
				return err
			}

			l.Infof("+ Success inspect %v cluster", color.RedString("[%v]", a.clusterName))
			l.Infof("  - Inspect report exported to %s, please download and view", color.GreenString("[%v]", fileName))
			if isAbnormal {
				l.Infof("  - Dev-Practices and DB-Statistics abnormal report exported to %s, please download and view", color.RedString("[%v]", abnormalFile))
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().Uint64Var(&a.sshTimeout, "ssh-timeout", 5, "Timeout in seconds to connect host via SSH, ignored for operations that don't need an SSH connection. (default 5)")
	cmd.Flags().Uint64Var(&a.waitTimeout, "wait-timeout", 120, "Timeout in seconds to wait for an operation to complete, ignored for operations that don't fit. (default 120)")
	cmd.Flags().StringVar(&a.ssh, "ssh", "builtin", "the ssh type: 'builtin', 'system', 'none'")
	cmd.Flags().StringVarP(&a.sshUser, "user", "u", "root", "The user name to login via SSH. The user must has root (or sudo) privilege. (default: root)")
	cmd.Flags().BoolVarP(&a.usePassword, "password", "p", false, " Use password of target hosts. If specified, password authentication will be used.")
	cmd.Flags().StringVarP(&a.identityFile, "identity", "i", "~/.ssh/id_rsa", "The path of the SSH identity file. If specified, public key authentication will be used. (default: ~/.ssh/id_rsa)")
	cmd.Flags().StringVarP(&a.output, "output", "o", "/tmp", "Configure the inspection report output directory")
	cmd.Flags().IntVar(&a.concurrency, "concurrency", 5, "max number of parallel tasks to run")
	return cmd
}
