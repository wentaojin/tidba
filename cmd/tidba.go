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
)

// App is storage for app's root command
type App struct {
	Host         string
	Port         string
	User         string
	Password     string
	DBName       string
	Concurrency  int // app run concurrency
	IncludeTable []string
	ExcludeTable []string
	All          bool
	RegexTable   string
	Version      string   // tidba app version
	Args         []string // Args set by sub commands
}

// Cmd returns a cobra.Command instance to be added to app's root command
func (app *App) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "tidba",
		Short:             "CLI tidba app for tidb cluster",
		PersistentPreRunE: app.PersistentPreRunE,
		SilenceUsage:      true,
	}
	cmd.PersistentFlags().StringVarP(&app.Host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().StringVarP(&app.Port, "port", "P", "4000", "database service port")
	cmd.PersistentFlags().StringVarP(&app.User, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.Password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.DBName, "db", "d", "", "database name")
	cmd.PersistentFlags().IntVarP(&app.Concurrency, "concurrency", "f", 5, "app concurrency")
	cmd.PersistentFlags().StringSliceVarP(&app.IncludeTable, "include", "i", nil, "configure table name")
	cmd.PersistentFlags().StringSliceVarP(&app.ExcludeTable, "exclude", "e", nil, "configure exclude table name")
	cmd.PersistentFlags().StringVarP(&app.RegexTable, "regex", "r", "", "configure table name by go regexp")
	cmd.PersistentFlags().BoolVar(&app.All, "all", false, "all tables in the database")
	cmd.PersistentFlags().StringVarP(&app.Version, "version", "v", "", "version for app")
	return cmd
}

// PersistentPreRunE is a global initializer for this app
func (app *App) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	return nil
}
