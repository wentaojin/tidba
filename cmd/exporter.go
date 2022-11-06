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
	"github.com/wentaojin/tidba/pkg/db"
	"github.com/wentaojin/tidba/pkg/exporter"
)

// AppExporter
type AppExporter struct {
	*App // embedded parent command storage
}

func (app *App) AppExporter() Cmder {
	return &AppExporter{App: app}
}

func (app *AppExporter) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "exporter",
		Short:        "Exporter used to exporter tidb cluster info",
		Long:         `Exporter used to exporter tidb cluster info`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (app *AppExporter) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
Exporter user info
*/
type AppExporterUser struct {
	*AppExporter    // embedded parent command storage
	host            string
	port            int
	user            string
	password        string
	dbName          string
	allUser         bool
	clusterVersion  int
	includeUsername []string
	excludeUsername []string
	regexpUsername  string
	outDir          string
}

func (app *AppExporter) AppExporterUser() Cmder {
	return &AppExporterUser{AppExporter: app}
}

func (app *AppExporterUser) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "user",
		Short:        "Exporter tidb cluster create user with password and privileges sql",
		Long:         `Exporter tidb cluster create user with password and privileges sql`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.Flags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.Flags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.Flags().StringVarP(&app.password, "password", "p", "", "database user password")
	cmd.Flags().StringVarP(&app.dbName, "db", "D", "", "database name")
	cmd.Flags().BoolVar(&app.allUser, "all-user", false, "exporter all user(no-root) info in the database")
	cmd.Flags().StringSliceVar(&app.includeUsername, "include-user", nil, "exporter designated user info in the database")
	cmd.Flags().StringSliceVar(&app.excludeUsername, "exclude-user", nil, "exporter other user info in the database")
	cmd.Flags().StringVar(&app.regexpUsername, "regexp-user", "", "exporter regexp user info in the database")
	cmd.Flags().IntVar(&app.clusterVersion, "cluster-version", 4, "configure cluster version, for example: 2、3、4、5")
	cmd.Flags().StringVarP(&app.outDir, "out-dir", "o", "/tmp/exporter", "exporter sql file output dir")
	return cmd
}

func (app *AppExporterUser) RunE(cmd *cobra.Command, args []string) error {
	engine, err := db.NewMysqlDSN(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	if app.allUser {
		if err := exporter.AllUserExporter(app.clusterVersion, app.outDir, engine); err != nil {
			return err
		}
	}
	if app.includeUsername != nil {
		if err := exporter.IncludeUserExporter(app.clusterVersion, app.includeUsername, app.outDir, engine); err != nil {
			return err
		}
	}
	if app.excludeUsername != nil {
		if err := exporter.FilterUserExporter(app.clusterVersion, app.excludeUsername, app.outDir, engine); err != nil {
			return err
		}
	}
	if app.regexpUsername != "" {
		if err := exporter.RegexpUserExporter(app.clusterVersion, app.regexpUsername, app.outDir, engine); err != nil {
			return err
		}
	}
	return nil
}
