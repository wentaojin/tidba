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
	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/exporter"
	"github.com/spf13/cobra"
)

// AppExporter
type AppExporter struct {
	*App       // embedded parent command storage
	KeyFormat  string
	TableID    int64
	IndexID    int64
	RowValue   string
	IndexValue string
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
	AllUser         bool
	ClusterVersion  int
	IncludeUsername []string
	ExcludeUsername []string
	RegexpUsername  string
	OutDir          string
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
	cmd.Flags().BoolVar(&app.AllUser, "all-user", false, "exporter all user(no-root) info in the database")
	cmd.Flags().StringSliceVar(&app.IncludeUsername, "include-user", nil, "exporter designated user info in the database")
	cmd.Flags().StringSliceVar(&app.ExcludeUsername, "exclude-user", nil, "exporter other user info in the database")
	cmd.Flags().StringVar(&app.RegexpUsername, "regexp-user", "", "exporter regexp user info in the database")
	cmd.Flags().IntVar(&app.ClusterVersion, "cluster-version", 4, "configure cluster version, for example: 2、3、4、5")
	cmd.Flags().StringVarP(&app.OutDir, "out-dir", "o", "/tmp/exporter", "exporter sql file output dir")
	return cmd
}

func (app *AppExporterUser) RunE(cmd *cobra.Command, args []string) error {
	engine, err := db.NewMysqlDSN(app.User, app.Password, app.Host, app.Port, app.DBName)
	if err != nil {
		return err
	}
	if app.All {
		if err := exporter.AllUserExporter(app.ClusterVersion, app.OutDir, engine); err != nil {
			return err
		}
	}
	if app.IncludeUsername != nil {
		if err := exporter.IncludeUserExporter(app.ClusterVersion, app.IncludeUsername, app.OutDir, engine); err != nil {
			return err
		}
	}
	if app.ExcludeUsername != nil {
		if err := exporter.FilterUserExporter(app.ClusterVersion, app.ExcludeUsername, app.OutDir, engine); err != nil {
			return err
		}
	}
	if app.RegexpUsername != "" {
		if err := exporter.RegexpUserExporter(app.ClusterVersion, app.RegexpUsername, app.OutDir, engine); err != nil {
			return err
		}
	}
	return nil
}
