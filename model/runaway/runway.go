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
package runaway

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/utils/stringutil"
)

type SqlRunawayModel struct {
	ctx           context.Context
	cancel        context.CancelFunc
	clusterName   string
	command       string
	sqlDigest     string
	resourceGroup string
	ruPerSec      int
	priority      string
	watchIDs      []int
	sqlText       string
	action        string
	spinner       spinner.Model
	mode          string
	Msgs          interface{}
	Error         error
}

func NewSqlRunawayModel(clusterName string,
	sqlDigest string,
	resourceGroup string,
	ruPerSec int,
	priority string, command, sqlText, action string, watchIDs []int) SqlRunawayModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return SqlRunawayModel{
		ctx:           ctx,
		cancel:        cancel,
		spinner:       sp,
		clusterName:   clusterName,
		resourceGroup: resourceGroup,
		ruPerSec:      ruPerSec,
		priority:      priority,
		command:       command,
		watchIDs:      watchIDs,
		sqlDigest:     sqlDigest,
		sqlText:       sqlText,
		action:        action,
		mode:          model.BubblesModeQuering,
	}
}

func (m SqlRunawayModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m SqlRunawayModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		default:
			return m, nil
		}
	case listRunawayMsg:
		m.mode = model.BubblesModeQueried
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msgs = msg.msgs
		}
		return m, tea.Quit
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	default:
		m.spinner, cmd = m.spinner.Update(msg)
		switch m.command {
		case "CREATE":
			return m, tea.Batch(
				cmd,
				submitRunawayCreateData(m.ctx, m.clusterName, m.resourceGroup, m.sqlDigest, m.priority, m.sqlText, m.action, m.ruPerSec), // submit list data
			)
		case "QUERY":
			return m, tea.Batch(
				cmd,
				submitRunawayQueryData(m.ctx, m.clusterName), // submit list data
			)
		case "DELETE":
			return m, tea.Batch(
				cmd,
				submitRunawayDeleteData(m.ctx, m.clusterName, m.watchIDs), // submit list data
			)
		default:
			return m, tea.Quit
		}
	}
}

func (m SqlRunawayModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Executing cluster topsql runaway information...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return "❌ Executed failed!\n\n"
		}
		return "✅ Executed successfully!\n\n"
	}
}

type listRunawayMsg struct {
	msgs interface{}
	err  error
}

type QueriedRespMsg struct {
	Columns []string
	Results [][]interface{}
}

func submitRunawayCreateData(ctx context.Context, clusterName string, resourceGroup string, sqlDigest string, priority, sqlText, action string, ruPerSec int) tea.Cmd {
	return func() tea.Msg {
		cols, results, err := TopsqlRunaway(ctx, clusterName, resourceGroup, sqlDigest, priority, sqlText, action, ruPerSec)
		if err != nil {
			return listRunawayMsg{err: err}
		}
		var rows [][]interface{}
		for _, r := range results {
			var row []interface{}
			for _, c := range cols {
				for k, v := range r {
					if c == k {
						row = append(row, v)
					}
				}
			}
			rows = append(rows, row)
		}
		return listRunawayMsg{msgs: &QueriedRespMsg{
			Columns: cols,
			Results: rows,
		}, err: nil}
	}
}

func submitRunawayQueryData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRunawayMsg{err: err}
		}
		db := connDB.(*mysql.Database)
		queryStr := `select version() AS VERSION`
		_, res, err := db.GeneralQuery(ctx, queryStr)
		if err != nil {
			return listRunawayMsg{err: fmt.Errorf("the query sql [%v] run failed: %v", queryStr, err)}
		}
		vers := strings.Split(res[0]["VERSION"], "-")

		if stringutil.VersionOrdinal(strings.TrimPrefix(vers[len(vers)-1], "v")) < stringutil.VersionOrdinal("8.5.0") {
			return listRunawayMsg{err: fmt.Errorf("the cluster [%s] database version [%s] not meet requirement, require version >= v8.5.0, need use SWITCH_GROUP feature", clusterName, vers[len(vers)-1])}
		}

		queryStr = `SELECT
rw.ID,
rw.RESOURCE_GROUP_NAME AS RESOURCE_GROUP,
rw.START_TIME,
rw.END_TIME,
rg.RU_PER_SEC,
rg.PRIORITY,
rg.QUERY_LIMIT,
rw.ACTION,
rw.WATCH_TEXT
FROM 
INFORMATION_SCHEMA.RUNAWAY_WATCHES rw,
INFORMATION_SCHEMA.RESOURCE_GROUPS rg
WHERE
rw.RESOURCE_GROUP_NAME = rg.NAME`
		cols, res, err := db.GeneralQuery(ctx, queryStr)
		if err != nil {
			return listRunawayMsg{err: fmt.Errorf("the query sql [%v] run failed: %v", queryStr, err)}
		}

		var rows [][]interface{}
		for _, r := range res {
			var row []interface{}
			for _, c := range cols {
				for k, v := range r {
					if c == k {
						row = append(row, v)
					}
				}
			}
			rows = append(rows, row)
		}
		return listRunawayMsg{msgs: &QueriedRespMsg{
			Columns: cols,
			Results: rows,
		}, err: nil}
	}
}

func submitRunawayDeleteData(ctx context.Context, clusterName string, ids []int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRunawayMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		_, res, err := db.GeneralQuery(ctx, `select version() AS VERSION`)
		if err != nil {
			return listRunawayMsg{err: err}
		}
		vers := strings.Split(res[0]["VERSION"], "-")

		if stringutil.VersionOrdinal(strings.TrimPrefix(vers[len(vers)-1], "v")) < stringutil.VersionOrdinal("8.5.0") {
			return listRunawayMsg{err: fmt.Errorf("the cluster [%s] database version [%s] not meet requirement, require version >= v8.5.0, need use SWITCH_GROUP feature", clusterName, vers[len(vers)-1])}
		}

		metaDB, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return listRunawayMsg{err: err}
		}
		meta := metaDB.(*sqlite.Database)

		rc, err := meta.GetResourceGroup(ctx, clusterName)
		if err != nil {
			return listRunawayMsg{err: err}
		}

		for _, id := range ids {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`QUERY WATCH REMOVE %d`, id)); err != nil {
				return listRunawayMsg{err: err}
			}
		}
		if rc.ResourceGroupName != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`DROP RESOURCE GROUP %s`, rc.ResourceGroupName)); err != nil {
				return listRunawayMsg{err: err}
			}
			if _, err := meta.DeleteResourceGroup(ctx, clusterName); err != nil {
				return listRunawayMsg{err: err}
			}
		}

		cols, res, err := db.GeneralQuery(ctx, `SELECT
rw.ID,
rw.RESOURCE_GROUP_NAME AS RESOURCE_GROUP,
rw.START_TIME,
rw.END_TIME,
rg.RU_PER_SEC,
rg.PRIORITY,
rg.QUERY_LIMIT,
rw.ACTION,
rw.WATCH_TEXT
FROM 
INFORMATION_SCHEMA.RUNAWAY_WATCHES rw,
INFORMATION_SCHEMA.RESOURCE_GROUPS rg
WHERE
rw.RESOURCE_GROUP_NAME = rg.NAME`)
		if err != nil {
			return listRunawayMsg{err: err}
		}

		var rows [][]interface{}
		for _, r := range res {
			var row []interface{}
			for _, c := range cols {
				for k, v := range r {
					if c == k {
						row = append(row, v)
					}
				}
			}
			rows = append(rows, row)
		}
		return listRunawayMsg{msgs: &QueriedRespMsg{
			Columns: cols,
			Results: rows,
		}, err: nil}
	}
}
