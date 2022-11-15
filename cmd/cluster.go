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
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/cluster"
	"github.com/wentaojin/tidba/db"
	"strings"
)

// AppCluster is storage for the sub command analyze
type AppCluster struct {
	*App     // embedded parent command storage
	host     string
	port     int
	user     string
	password string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *App) AppCluster() Cmder {
	return &AppCluster{App: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppCluster) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "cluster",
		Short:            "Operator cluster info",
		Long:             `Operator cluster info`,
		RunE:             app.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.PersistentFlags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.password, "password", "p", "", "database user password")
	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppCluster) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
Cluster region info
*/
type AppClusterRegion struct {
	*AppCluster
	regionID    []string
	peers       string
	pdAddr      string
	regionType  string
	replicaType string
	downTiKVS   []string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *AppCluster) AppClusterRegion() Cmder {
	return &AppClusterRegion{AppCluster: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppClusterRegion) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "region",
		Short:        "Operator cluster region info",
		Long:         `Operator cluster region info`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringSliceVar(&app.regionID, "regionid", nil, "configure region peer nums")
	cmd.Flags().StringVar(&app.peers, "peers", "", "configure region peer nums")
	cmd.Flags().StringVar(&app.pdAddr, "pdAddr", "127.0.0.1:2379", "configure cluster pd addr")
	cmd.Flags().StringVar(&app.regionType, "regiontype", "all", "configure cluster region type (data/index/all)")
	cmd.Flags().StringVar(&app.replicaType, "replicatype", "", "configure cluster region replica problem type (majorDown/lessDown)")
	cmd.Flags().StringSliceVarP(&app.downTiKVS, "downtikv", "s", nil, "configure tikv addr (tikvIP:servicePort)")
	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppClusterRegion) RunE(cmd *cobra.Command, args []string) error {
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, "")
	if err != nil {
		return err
	}

	switch {
	case app.peers != "" && app.downTiKVS == nil && app.replicaType == "":
		if err := cluster.GetCurrentRegionPeer(app.peers, app.regionType, app.pdAddr, engine); err != nil {
			return err
		}

	case app.downTiKVS != nil && app.peers == "" && strings.EqualFold(app.replicaType, "majorDown"):
		if err := cluster.GetMajorDownRegionPeer(app.regionType, app.pdAddr, app.downTiKVS, engine); err != nil {
			return err
		}

	case app.downTiKVS != nil && app.peers == "" && strings.EqualFold(app.replicaType, "lessDown"):
		if err := cluster.GetLessDownRegionPeer(app.regionType, app.pdAddr, app.downTiKVS, engine); err != nil {
			return err
		}

	case app.regionID != nil && app.downTiKVS == nil && app.peers == "" && app.replicaType == "" && app.regionType == "all":
		if err := cluster.GetRegionIDINFO(app.regionID, app.regionType, app.pdAddr, engine); err != nil {
			return err
		}

	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
