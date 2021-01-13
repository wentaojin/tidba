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

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/diff"

	"github.com/spf13/cobra"
)

// AppDiff is storage for the sub command analyze
type AppDiff struct {
	*App // embedded parent command storage
}

func (app *App) AppDiff() Cmder {
	return &AppDiff{App: app}
}

func (app *AppDiff) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "diff",
		Short:        "Diff tidb cluster component conf before cluster upgrade",
		Long:         `Diff tidb cluster component conf before cluster upgrade`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (app *AppDiff) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

/*
	Component pd diff
*/
type AppDiffPD struct {
	*AppDiff   // embedded parent command storage
	basePDAddr string
	newPDAddr  string
	format     string
	coloring   bool
	quiet      bool
}

func (app *AppDiff) AppDiffPD() Cmder {
	return &AppDiffPD{AppDiff: app}
}

func (app *AppDiffPD) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "pd",
		Short:        "Diff component pd conf before cluster upgrade",
		Long:         `Diff component pd conf before cluster upgrade`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVar(&app.basePDAddr, "base-pd-addr", "", "configure cluster base pd addr, for example: pd-ip:status-port")
	cmd.Flags().StringVar(&app.newPDAddr, "new-pd-addr", "", "configure cluster new pd addr, for example: pd-ip:status-port")
	cmd.Flags().StringVar(&app.format, "format", "ascii", "configure diff output format (ascii, delta)")
	cmd.Flags().BoolVar(&app.coloring, "coloring", false, "enable coloring in the ASCII mode (not available in the delta mode)")
	cmd.Flags().BoolVar(&app.quiet, "quiet", false, "Quiet output, if no differences are found")
	return cmd
}

func (app *AppDiffPD) RunE(cmd *cobra.Command, args []string) error {
	msg := "flag `%s` is requirement, can not null"
	if app.basePDAddr == "" {
		return fmt.Errorf(msg, "base-pd-addr")
	}
	if app.newPDAddr == "" {
		return fmt.Errorf(msg, "new-pd-addr")
	}

	err := diff.ComponentPDDiff(app.basePDAddr, app.newPDAddr, app.format, app.coloring, app.quiet)
	if err != nil && err == diff.Equivalent {
		zlog.Logger.Info("Task run success", zap.String("equivalent",
			fmt.Sprintf(`the pd components on both sides of [%s] and [%s] have the same configuration,so you can skip the check`, app.basePDAddr, app.newPDAddr)))
	} else {
		return err
	}
	return nil
}

/*
	Component tidb diff
*/
type AppDiffTiDB struct {
	*AppDiff         // embedded parent command storage
	baseTiDBAddr     string
	baseTiDBUser     string
	baseTiDBPassword string
	newTiDBAddr      string
	newTiDBUser      string
	newTiDBPassword  string
	diffType         string
	format           string
	coloring         bool
	quiet            bool
}

func (app *AppDiff) AppDiffTiDB() Cmder {
	return &AppDiffTiDB{AppDiff: app}
}

func (app *AppDiffTiDB) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "tidb",
		Short:        "Diff component tidb conf before cluster upgrade",
		Long:         `Diff component tidb conf before cluster upgrade`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVar(&app.baseTiDBAddr, "base-tidb-addr", "", "configure cluster base tidb addr, for example: tidb-ip:port")
	cmd.Flags().StringVar(&app.baseTiDBUser, "base-tidb-user", "", "configure cluster base tidb db user")
	cmd.Flags().StringVar(&app.baseTiDBPassword, "base-tidb-pass", "", "configure cluster base tidb db user password")
	cmd.Flags().StringVar(&app.newTiDBAddr, "new-tidb-addr", "", "configure cluster new tidb addr, for example: tidb-ip:port")
	cmd.Flags().StringVar(&app.newTiDBUser, "new-tidb-user", "", "configure cluster new tidb db user")
	cmd.Flags().StringVar(&app.newTiDBPassword, "new-tidb-pass", "", "configure cluster new tidb db user password")
	cmd.Flags().StringVar(&app.diffType, "diff-type", "variable", "configure tidb diff type; can be 'variable' (default if omitted), or 'config'")
	cmd.Flags().StringVar(&app.format, "format", "ascii", "configure diff output format (ascii, delta)")
	cmd.Flags().BoolVar(&app.coloring, "coloring", false, "enable coloring in the ASCII mode (not available in the delta mode)")
	cmd.Flags().BoolVar(&app.quiet, "quiet", false, "Quiet output, if no differences are found")
	return cmd
}

func (app *AppDiffTiDB) RunE(cmd *cobra.Command, args []string) error {
	if err := app.validateParameters(); err != nil {
		return err
	}
	err := diff.ComponentTiDBDiff(app.baseTiDBAddr, app.baseTiDBUser, app.baseTiDBPassword, app.newTiDBAddr,
		app.newTiDBUser, app.newTiDBPassword, app.diffType, app.format, app.coloring, app.quiet)
	if err != nil && err == diff.Equivalent {
		zlog.Logger.Info("Task run success", zap.String("equivalent",
			fmt.Sprintf(`the tidb components on both sides of [%s] and [%s] have the same configuration,so you can skip the check`, app.baseTiDBAddr, app.newTiDBAddr)))
	} else {
		return err
	}
	return nil
}

func (app *AppDiffTiDB) validateParameters() error {
	msg := "flag `%s` is requirement, can not null"
	if app.baseTiDBAddr == "" {
		return fmt.Errorf(msg, "base-addr")
	}
	if app.baseTiDBUser == "" {
		return fmt.Errorf(msg, "base-user")
	}
	if app.newTiDBAddr == "" {
		return fmt.Errorf(msg, "new-addr")
	}
	if app.newTiDBUser == "" {
		return fmt.Errorf(msg, "new-user")
	}
	return nil
}

/*
	Component tikv diff
*/
type AppDiffTiKV struct {
	*AppDiff         // embedded parent command storage
	baseTiKVAddr     string
	baseTiKVJsonFile string
	newTiKVAddr      string
	format           string
	coloring         bool
	quiet            bool
}

func (app *AppDiff) AppDiffTiKV() Cmder {
	return &AppDiffTiKV{AppDiff: app}
}

func (app *AppDiffTiKV) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "tikv",
		Short:        "Diff component tikv conf before cluster upgrade",
		Long:         `Diff component tikv conf before cluster upgrade`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&app.baseTiKVAddr, "base-tikv-addr", "", "configure cluster base tikv addr, general used to be higher v4.0 cluster,for example: tikv-ip:status-port")
	cmd.Flags().StringVar(&app.baseTiKVJsonFile, "base-tikv-json", "", "configure cluster base tikv json, general used to be lower v4.0 cluster,for example: v3.0.5.json")
	cmd.Flags().StringVar(&app.newTiKVAddr, "new-tikv-addr", "", "configure cluster new tikv addr, general used to be higher v4.0 cluster,for example: tikv-ip:status-port")
	cmd.Flags().StringVar(&app.format, "format", "ascii", "configure diff output format (ascii, delta)")
	cmd.Flags().BoolVar(&app.coloring, "coloring", false, "enable coloring in the ASCII mode (not available in the delta mode)")
	cmd.Flags().BoolVar(&app.quiet, "quiet", false, "Quiet output, if no differences are found")

	return cmd
}

func (app *AppDiffTiKV) RunE(cmd *cobra.Command, args []string) error {
	switch {
	case app.baseTiKVAddr != "" && app.newTiKVAddr != "":
		err := diff.ComponentTiKVDiffByAPI(app.baseTiKVAddr, app.newTiKVAddr, app.format, app.coloring, app.quiet)
		if err != nil && err == diff.Equivalent {
			zlog.Logger.Info("Task run success", zap.String("equivalent",
				fmt.Sprintf(`the tikv components on both sides of [%s] and [%s] have the same configuration,so you can skip the check`, app.baseTiKVAddr, app.newTiKVAddr)))
		} else {
			return err
		}
	case app.baseTiKVJsonFile != "" && app.newTiKVAddr != "":
		err := diff.ComponentTiKVDiffByJSON(app.baseTiKVJsonFile, app.newTiKVAddr, app.format, app.coloring, app.quiet)
		if err != nil && err == diff.Equivalent {
			zlog.Logger.Info("Task run success", zap.String("equivalent",
				fmt.Sprintf(`the tikv components on both sides of [%s] and [%s] have the same configuration,so you can skip the check`, app.baseTiKVJsonFile, app.newTiKVAddr)))
		} else {
			return err
		}
	default:
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	return nil
}
