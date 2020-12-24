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

	"github.com/WentaoJin/tidba/pkg/sql"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/spf13/cobra"
)

// AppSQL is storage for the sub command analyze
type AppSQL struct {
	*App         // embedded parent command storage
	SQLFile      string
	SQLSeparator string
	OutDir       string
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
	cmd.Flags().StringVar(&app.SQLFile, "sql-file", "", "configure sql file")
	cmd.Flags().StringVar(&app.SQLSeparator, "sql-separator", ";", "configure sql separator in th sql file")
	cmd.Flags().StringVar(&app.OutDir, "out-dir", "/tmp/sql/", "configure sql output dir")
	return cmd
}

func (app *AppSQL) RunE(cmd *cobra.Command, args []string) error {
	if app.SQLFile == "" {
		return fmt.Errorf("flag sql-file is requirement, can not null")
	}
	engine, err := db.NewMysqlDSN(app.User, app.Password, app.Host, app.Port, app.DBName)
	if err != nil {
		return err
	}
	return sql.RunSQL(engine, app.SQLFile, app.SQLSeparator, app.OutDir)
}
