package main

import (
	"context"
	"fmt"
	"log"

	"github.com/fatih/color"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/utils/stringutil"
	"github.com/wentaojin/tidba/utils/version"
)

func init() {
	database.Connector = database.NewDBConnector()
}

type App struct {
	database           string
	clusterName        string
	disableInteractive bool
	version            bool
	histFile           string
}

func (a *App) Cmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "tidba",
		Long: "TiDBA (tidba) is a CLI for tidb distributed data dba operation and maintenance, which can quickly analyze, diagnose and troubleshoot problems.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			dir, err := homedir.Expand(a.database)
			if err != nil {
				return err
			}
			err = stringutil.PathNotExistOrCreate(dir)
			if err != nil {
				return err
			}

			connector, err := database.CreateConnector(context.Background(), &database.ClusterConfig{
				DbType: database.DatabaseTypeSqlite,
				DSN:    dir,
			})
			if err != nil {
				return err
			}
			database.Connector.AddDatabase(database.DefaultSqliteClusterName, connector)

			if !a.disableInteractive {
				a.histFile = fmt.Sprintf("%s/tidba_history", dir)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if a.version {
				fmt.Println(version.GetRawVersionInfo())
				return nil
			}

			return cmd.Help()
		},
		SilenceErrors:    true,
		SilenceUsage:     true,
		TraverseChildren: true,
	}

	rootCmd.PersistentFlags().StringVarP(&a.database, "database", "D", "~/.tidba", "location of the tidba metadata database")
	rootCmd.PersistentFlags().StringVarP(&a.clusterName, "cluster", "c", "", "configure the cluster name that tidba needs to operate")
	rootCmd.Flags().BoolVarP(&a.disableInteractive, "disable-interactive", "d", false, "interactive for the tidba application (default: interactive mode)")
	rootCmd.Flags().BoolVarP(&a.version, "version", "v", false, "version for the tidba application")

	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fgGreen := color.New(color.FgGreen)
		fmt.Println(fgGreen.Sprint(cmd.UsageString()))
	})

	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage clusters",
	}

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Root().PersistentPreRunE(cmd, args); err != nil {
				return err
			}
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			_, err := database.Connector.GetDatabase(a.clusterName)
			if err != nil {
				return err
			}
			return nil
		},
	}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(239448)
			// 插入执行逻辑
			return nil
		},
	}

	// Ensure all subcommands of inspectCmd inherit PersistentPreRunE
	inspectCmd.AddCommand(createCmd)
	clusterCmd.AddCommand(inspectCmd)
	rootCmd.AddCommand(clusterCmd)

	return rootCmd
}

func main() {
	app := &App{}
	if err := app.Cmd().Execute(); err != nil {
		log.Fatal(err)
	}
}
