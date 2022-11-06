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

	"github.com/wentaojin/tidba/pkg/sql"

	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/pkg/db"
)

// AppSQL is storage for the sub command analyze
type AppSQL struct {
	*App         // embedded parent command storage
	host         string
	port         int
	user         string
	password     string
	dbName       string
	sqlFile      string
	sqlSeparator string
	outDir       string
}

func (app *App) AppSQL() Cmder {
	return &AppSQL{App: app}
}

func (app *AppSQL) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "sql",
		Short:        "SQL run result output in the tidb cluster",
		Long:         `SQL run result output in the tidb cluster`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.PersistentFlags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.dbName, "db", "D", "", "database name")
	cmd.Flags().StringVar(&app.sqlFile, "sql-file", "", "configure sql file")
	cmd.Flags().StringVar(&app.sqlSeparator, "sql-separator", ";", "configure sql separator in th sql file")
	cmd.Flags().StringVar(&app.outDir, "out-dir", "/tmp/sql/", "configure sql output dir")
	return cmd
}

func (app *AppSQL) RunE(cmd *cobra.Command, args []string) error {
	if app.sqlFile == "" {
		return fmt.Errorf("flag sql-file is requirement, can not null")
	}
	engine, err := db.NewMysqlDSN(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	return sql.RunSQL(engine, app.sqlFile, app.sqlSeparator, app.outDir)
}
