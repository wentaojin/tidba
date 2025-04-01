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
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/model/kill"
)

type AppKill struct {
	*App

	duration    int
	interval    int
	concurrency int
}

func (a *App) AppKill() Cmder {
	return &AppKill{App: a}
}

func (a *AppKill) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kill",
		Short: "Kill used to kill cluster database sessions or SQL operations",
		Long:  "Kill used to kill cluster database sessions or SQL operations where the specified cluster name is located",
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

	cmd.PersistentFlags().IntVar(&a.duration, "duration", 30, "configure the duration of the cluster database kill operation, If you set it to 0, it means continuous kill. size: seconds")
	cmd.PersistentFlags().IntVar(&a.interval, "interval", 500, "configure the duration of the cluster database kill operation interval, minimum interval 500. size: millisecond")
	cmd.PersistentFlags().IntVar(&a.concurrency, "concurrency", 5, "configure the duration of the cluster database kill operation concurrency")
	return cmd
}

type AppKillSql struct {
	*AppKill

	sqlDigests []string
}

func (a *AppKill) AppKillSql() Cmder {
	return &AppKillSql{AppKill: a}
}

func (a *AppKillSql) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "SQL kill session operation based on sql digest intermittently or continuously",
		Long:  "SQL kill session operation based on sql digest intermittently or continuously where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if len(a.sqlDigests) == 0 {
				return fmt.Errorf(`the sql-digests cannot be empty, required flag(s) --sql-digests {sqlDigest1,sqlDigest2} not set`)
			}
			if a.interval < 500 {
				a.interval = 500
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return kill.GenerateKillSessionSqlBySqlDigest(context.Background(), a.clusterName, a.sqlDigests, a.duration, a.interval, a.concurrency)

		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().StringSliceVar(&a.sqlDigests, "sql-digests", nil, "configure the cluster database kill sql digest")
	return cmd
}

type AppKillUsername struct {
	*AppKill

	usernames []string
}

func (a *AppKill) AppKillUsername() Cmder {
	return &AppKillUsername{AppKill: a}
}

func (a *AppKillUsername) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "User kill session operation based on username sql digest intermittently or continuously",
		Long:  "User kill session operation based on username sql digest intermittently or continuously where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if len(a.usernames) == 0 {
				return fmt.Errorf(`the users cannot be empty, required flag(s) --users {user1,user2} not set`)
			}
			if a.interval < 500 {
				a.interval = 500
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return kill.GenerateKillSessionSqlByUsername(context.Background(), a.clusterName, a.usernames, a.duration, a.interval, a.concurrency)
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.Flags().StringSliceVar(&a.usernames, "users", nil, "configure the cluster database kill usernames")
	return cmd
}
