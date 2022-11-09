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
	"github.com/wentaojin/tidba/db"
	"github.com/wentaojin/tidba/store"
	"strings"
)

// AppStore is storage for the sub command analyze
type AppStore struct {
	*App          // embedded parent command storage
	host          string
	port          int
	user          string
	password      string
	dbName        string
	all           bool
	includeStores []string
	excludeStores []string
	regexStores   string
}

// is a method of App that returns a Cmder instance for the sub command
func (app *App) AppStore() Cmder {
	return &AppStore{App: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppStore) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "store",
		Short:            "Operator tikv store",
		Long:             `Operator tikv store`,
		RunE:             app.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.PersistentFlags().StringVarP(&app.host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.user, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.dbName, "db", "D", "", "database name")
	cmd.PersistentFlags().StringSliceVarP(&app.includeStores, "include", "i", nil, "configure store id")
	cmd.PersistentFlags().StringSliceVarP(&app.excludeStores, "exclude", "e", nil, "configure exclude store id")
	cmd.PersistentFlags().StringVarP(&app.regexStores, "regex", "r", "", "configure store id by go regexp")
	cmd.PersistentFlags().BoolVar(&app.all, "all", false, "all tables in the database")
	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppStore) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
	Table data or index store hotspot top distribution
*/
// includeTable、excludeTable、regexTable only one of the three
type AppStoreHotspot struct {
	*AppStore
	hotType string
	limit   int
}

// is a method of App that returns a Cmder instance for the sub command
func (app *AppStore) AppStoreHotspot() Cmder {
	return &AppStoreHotspot{AppStore: app}
}

// Cmd returns a cobra.Command instance to be added to the parent command
func (app *AppStoreHotspot) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "hotspot",
		Short:        "View table data or index store hotspot top distribution",
		Long:         `View table data or index store hotspot top distribution`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&app.hotType, "hottype", "t", "read", "configure view hotspot type (read/write)")
	cmd.Flags().IntVarP(&app.limit, "limit", "l", 10, "configure view hotspot top limit")

	return cmd
}

// RunE is a main routine of the sub command, returning a nil
func (app *AppStoreHotspot) RunE(cmd *cobra.Command, args []string) error {
	engine, err := db.NewMySQLEngine(app.user, app.password, app.host, app.port, app.dbName)
	if err != nil {
		return err
	}
	if !engine.IsExistDbName(app.dbName) {
		return err
	}

	if app.all {
		if strings.EqualFold(app.dbName, "") {
			if err := store.AllDBDataAndIndexStoreHotspotView(app.hotType, app.limit, engine); err != nil {
				return err
			}
		} else {
			if err := store.AllTableDataAndIndexStoreHotspotView(app.dbName, app.hotType, app.limit, engine); err != nil {
				return err
			}
		}
		return nil
	}
	switch {
	case app.includeStores != nil && app.excludeStores == nil && app.regexStores == "":
		if err := store.IncludeStoreRegionHotspotView(app.hotType, app.limit, app.Concurrency, app.includeStores, engine); err != nil {
			return err
		}

	case app.includeStores == nil && app.excludeStores != nil && app.regexStores == "":
		if err := store.FilterStoreRegionHotspotView(app.hotType, app.limit, app.Concurrency, app.excludeStores, engine); err != nil {
			return err
		}

	case app.includeStores == nil && app.excludeStores == nil && app.regexStores != "":
		if err := store.RegexpStoreRegionHotspotView(app.hotType, app.limit, app.Concurrency, app.regexStores, engine); err != nil {
			return err
		}

	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
