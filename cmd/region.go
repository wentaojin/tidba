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
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/tidwall/pretty"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/model/region"
	"github.com/wentaojin/tidba/utils/stringutil"
)

type AppRegion struct {
	*App
	database string
	stores   []string
	tables   []string
	indexes  []string
}

func (a *App) AppRegion() Cmder {
	return &AppRegion{App: a}
}

func (a *AppRegion) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "region",
		Short: "Display the cluster database table region",
		Long:  "Display the cluster database table region where the specified cluster name is located",
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
	cmd.PersistentFlags().StringSliceVar(&a.tables, "tables", nil, "configure the cluster database display specifying some table names")
	cmd.PersistentFlags().StringSliceVar(&a.stores, "stores", nil, "configure the cluster database display specifying some store address, require tikv store [ip:service_port]")
	cmd.PersistentFlags().StringSliceVar(&a.indexes, "indexes", nil, "configure the cluster database display specifying table index names")
	return cmd
}

type AppRegionHotspot struct {
	*AppRegion

	hottype string
	top     int
}

func (a *AppRegion) AppRegionHotspot() Cmder {
	return &AppRegionHotspot{AppRegion: a}
}

func (a *AppRegionHotspot) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hotspot",
		Short: "Hotspot display the cluster database table region",
		Long:  "Hotspot didplay the cluster database table region where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if a.database == "" {
				return fmt.Errorf(`the database cannot be empty, required flag(s) --database {databaseName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(region.NewRegionQueryModel(
				a.clusterName,
				a.database,
				a.stores,
				a.tables,
				a.indexes,
				a.hottype,
				a.top,
				"HOTSPOT",
				nil,
				"",
				"",
				0))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(region.RegionQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil && len(lModel.Msgs.([]*region.QueriedRespMsg)) > 0 {
				fmt.Println("cluster hotspot region content:")
				for _, m := range lModel.Msgs.([]*region.QueriedRespMsg) {
					if err := model.QueryResultFormatTableStyle(m.Columns, m.Results); err != nil {
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

	cmd.Flags().StringVar(&a.hottype, "type", "all", "configure the cluster database display hotspot type, options: read,write,all")
	cmd.Flags().IntVar(&a.top, "top", 10, "configure the cluster database display hotspot top results")
	return cmd
}

type AppRegionLeader struct {
	*AppRegion
}

func (a *AppRegion) AppRegionLeader() Cmder {
	return &AppRegionLeader{AppRegion: a}
}

func (a *AppRegionLeader) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "leader",
		Short: "Leader display the cluster database table region distributed",
		Long:  "Leader didplay the cluster database table region distributed where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if a.database == "" {
				return fmt.Errorf(`the database cannot be empty, required flag(s) --database {databaseName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(region.NewRegionQueryModel(
				a.clusterName,
				a.database,
				a.stores,
				a.tables,
				a.indexes,
				"",
				0,
				"LEADER",
				nil,
				"",
				"",
				0))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(region.RegionQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil && len(lModel.Msgs.([]*region.QueriedRespMsg)) > 0 {
				fmt.Println("cluster region leader distributed content:")
				for _, m := range lModel.Msgs.([]*region.QueriedRespMsg) {
					if err := model.QueryResultFormatTableStyle(m.Columns, m.Results); err != nil {
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

type AppRegionReplica struct {
	*AppRegion
	pdAddr      string
	regionType  string
	concurrency int
	daemon      bool
}

func (a *AppRegion) AppRegionReplica() Cmder {
	return &AppRegionReplica{AppRegion: a}
}

func (a *AppRegionReplica) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replica",
		Short: "Replica display the cluster database table major replica down region",
		Long:  "Replica didplay the cluster database table major replica down region distributed where the specified cluster name is located",
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
			var opts []tea.ProgramOption

			if a.daemon || !isatty.IsTerminal(os.Stdout.Fd()) {
				// If we're in daemon mode don't render the TUI
				opts = []tea.ProgramOption{tea.WithoutRenderer()}
				model.NewConsoleOutput()
			} else {
				// If we're in TUI mode, discard log output
				model.NewDisableConsoleOutput()
			}

			p := tea.NewProgram(
				region.NewRegionQueryModel(
					a.clusterName,
					a.database,
					a.stores,
					a.tables,
					a.indexes,
					"",
					0,
					"REPLICA",
					nil,
					a.regionType,
					a.pdAddr,
					a.concurrency),
				opts...)

			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(region.RegionQueryModel)
			if lModel.Error == nil && lModel.Msgs != nil {
				resp := lModel.Msgs.(*region.MajorityResp)
				if reflect.DeepEqual(resp, &region.MajorityResp{}) {
					fmt.Println("the cluster region replica peers normal, not found major down peers, please ignore and skip")
					return nil
				}

				fmt.Println("cluster region down majority content:")

				var rows []interface{}
				rows = append(rows, resp.ReplicaCounts)
				rows = append(rows, resp.RegionCounts)
				rows = append(rows, resp.DownRegionQueryCounts)
				rows = append(rows, resp.DownRegionEffectiveCounts)

				if err := model.QueryResultFormatTableStyleWithRowsArray(
					[]string{"cluster replica count", "total region count", "down region query count", "down region effective count"},
					append([][]interface{}{}, rows)); err != nil {
					return err
				}
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.TableHeader, resp.TableRows); err != nil {
					return err
				}

				if len(resp.DownRegionPanics) > 0 {
					fmt.Println("cluster region down majority panics content:")
					for _, rs := range stringutil.ArrayStringGroups(resp.DownRegionPanics, 10) {
						fmt.Println(strings.Join(rs, ","))
					}
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.pdAddr, "pd-addr", "", "configure the cluster pd server address, require: ip:service_port")
	cmd.Flags().StringVar(&a.regionType, "region-type", "all", "configure the cluster database replica region type, option: data/index/all")
	cmd.Flags().IntVar(&a.concurrency, "concurrency", 5, "configure the cluster database replica query concurrency")
	cmd.Flags().BoolVar(&a.daemon, "daemon", false, "configure the cluster database table split run as a daemon")

	return cmd
}

type AppRegionQuery struct {
	*AppRegion
	pdAddr      string
	regionIds   []string
	concurrency int
}

func (a *AppRegion) AppRegionQuery() Cmder {
	return &AppRegionQuery{AppRegion: a}
}

func (a *AppRegionQuery) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query display the cluster database table some region inforamtion",
		Long:  "Query didplay the cluster database table some region information where the specified cluster name is located",
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
			p := tea.NewProgram(region.NewRegionQueryModel(
				a.clusterName,
				a.database,
				a.stores,
				a.tables,
				a.indexes,
				"",
				0,
				"QUERY",
				a.regionIds,
				"",
				a.pdAddr,
				a.concurrency))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(region.RegionQueryModel)

			if lModel.Error == nil && lModel.Msgs != nil && len(lModel.Msgs.([]*region.SingleRegion)) > 0 {
				fmt.Println("cluster regions query content:")
				for _, rs := range lModel.Msgs.([]*region.SingleRegion) {
					fmt.Println("------")
					jsonByte, err := json.Marshal(rs)
					if err != nil {
						return err
					}
					fmt.Printf("%s", pretty.Pretty(jsonByte))
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.pdAddr, "pd-addr", "", "configure the cluster pd server address, require: ip:service_port")
	cmd.Flags().StringSliceVar(&a.regionIds, "region-ids", nil, "configure the cluster database query region ids")
	cmd.Flags().IntVar(&a.concurrency, "concurrency", 5, "configure the cluster database replica query concurrency")
	cmd.MarkFlagRequired("region-ids")
	return cmd
}
