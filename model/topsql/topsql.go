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
package topsql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/model"
)

type TopsqlQueryModel struct {
	ctx              context.Context
	cancel           context.CancelFunc
	clusterName      string
	nearly           int
	enableHistory    bool
	enableSqlDisplay bool
	startTime        string
	endTime          string
	top              int
	command          string
	concurrency      int
	component        string
	spinner          spinner.Model
	instances        []string
	mode             string
	Msgs             interface{}
	Error            error
}

func NewTopsqlQueryModel(clusterName string, nearly int,
	enableHistory bool,
	startTime string,
	endTime string,
	top int,
	command string,
	concurrency int,
	component string,
	enableSqlDisplay bool,
	instances []string,
) TopsqlQueryModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return TopsqlQueryModel{
		ctx:              ctx,
		cancel:           cancel,
		spinner:          sp,
		clusterName:      clusterName,
		nearly:           nearly,
		enableHistory:    enableHistory,
		startTime:        startTime,
		endTime:          endTime,
		top:              top,
		command:          command,
		concurrency:      concurrency,
		component:        component,
		enableSqlDisplay: enableSqlDisplay,
		instances:        instances,
		mode:             model.BubblesModeQuering,
	}
}

func (m TopsqlQueryModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m TopsqlQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case listRespMsg:
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
		case "ELAPSED":
			return m, tea.Batch(
				cmd,
				submitElapsedData(m.ctx, m.clusterName, m.nearly, m.enableHistory, m.enableSqlDisplay, m.startTime, m.endTime, m.top), // submit list data
			)
		case "EXECUTIONS":
			return m, tea.Batch(
				cmd,
				submitExecutionsData(m.ctx, m.clusterName, m.nearly, m.enableHistory, m.enableSqlDisplay, m.startTime, m.endTime, m.top), // submit list data
			)
		case "PLANS":
			return m, tea.Batch(
				cmd,
				submitPlansData(m.ctx, m.clusterName, m.nearly, m.enableHistory, m.enableSqlDisplay, m.startTime, m.endTime, m.top), // submit list data
			)
		case "CPU":
			return m, tea.Batch(
				cmd,
				submitCpuData(m.ctx, m.clusterName, m.nearly, m.startTime, m.endTime, m.top, m.component, m.concurrency, m.enableSqlDisplay, m.instances), // submit list data
			)
		default:
			return m, tea.Quit
		}
	}
}

func (m TopsqlQueryModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster topsql information...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return fmt.Sprintf("\n❌ Queried error: %s\n", m.Error.Error())
		}
		return "✅ Queried successfully!\n\n"
	}
}

type listRespMsg struct {
	msgs interface{}
	err  error
}
type QueriedRespMsg struct {
	Columns []string
	Results [][]interface{}
}

func submitElapsedData(ctx context.Context, clusterName string, nearly int,
	enableHistory bool,
	enableSqlDisplay bool,
	startTime string,
	endTime string,
	top int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		totalLatencySql, err := GenerateQueryWindowSqlElapsedTime(nearly, startTime, endTime, enableHistory)
		if err != nil {
			return listRespMsg{err: err}
		}

		_, res, err := db.GeneralQuery(ctx, totalLatencySql)
		if err != nil {
			return listRespMsg{err: err}
		}
		var totalLatency string
		if len(res) == 0 {
			return listRespMsg{err: fmt.Errorf("the database sql [%v] query time windows result not found", totalLatencySql)}
		} else {
			totalLatency = res[0]["all_latency_s"]
		}

		queries, err := GenerateTopsqlElapsedTimeQuery(nearly, top, startTime, endTime, enableHistory, totalLatency)
		if err != nil {
			return listRespMsg{err: err}
		}

		cols, res, err := db.GeneralQuery(ctx, queries)
		if err != nil {
			return listRespMsg{err: err}
		}

		columns := []string{"Elapsed Time(s)", "Executions", "Elap per Exec(s)", "Min query Time(s)", "Max query Time(s)", "Avg total keys", "Avg processed keys", `% Total SQL Time`, "SQL Digest"}

		if enableSqlDisplay {
			columns = append(columns, "SQL Text")
		}

		var rows [][]interface{}
		for _, r := range res {
			// exclude sort column name
			var row []interface{}
			for ind, c := range cols[1:] {
				for k, v := range r {
					if c == k {
						// total sql time percent
						if ind == 7 {
							percent, err := strconv.ParseFloat(v, 64)
							if err != nil {
								return listRespMsg{err: err}
							}
							row = append(row, fmt.Sprintf("%v%%", percent*100))
						} else if ind == len(cols[1:])-1 {
							if enableSqlDisplay {
								row = append(row, v)
							}
						} else {
							row = append(row, v)
						}
					}
				}
			}
			rows = append(rows, row)
		}
		return listRespMsg{msgs: &QueriedRespMsg{
			Columns: columns,
			Results: rows,
		}, err: nil}
	}
}

func submitExecutionsData(ctx context.Context, clusterName string, nearly int,
	enableHistory bool,
	enableSqlDisplay bool,
	startTime string,
	endTime string,
	top int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		totalLatencySql, err := GenerateQueryWindowSqlElapsedTime(nearly, startTime, endTime, enableHistory)
		if err != nil {
			return listRespMsg{err: err}
		}

		_, res, err := db.GeneralQuery(ctx, totalLatencySql)
		if err != nil {
			return listRespMsg{err: err}
		}
		var totalLatency string
		if len(res) == 0 {
			return listRespMsg{err: fmt.Errorf("the database sql [%v] query time windows result not found", totalLatencySql)}
		} else {
			totalLatency = res[0]["all_latency_s"]
		}

		queries, err := GenerateTopsqlExecutionsQuery(nearly, top, startTime, endTime, enableHistory, totalLatency)
		if err != nil {
			return listRespMsg{err: err}
		}

		cols, res, err := db.GeneralQuery(ctx, queries)
		if err != nil {
			return listRespMsg{err: err}
		}

		columns := []string{"Executions", "Elap per Exec(s)", "Parse Per Exec(s)", "Compile Per Exec(s)", "Min query Time(s)", "Max query Time(s)", "Avg total keys", "Avg processed keys", `% Total SQL Time`, "SQL Digest"}

		if enableSqlDisplay {
			columns = append(columns, "SQL Text")
		}

		var rows [][]interface{}
		for _, r := range res {
			// exclude sort column name
			var row []interface{}
			for ind, c := range cols[1:] {
				for k, v := range r {
					if c == k {
						// total sql time percent
						if ind == 8 {
							percent, err := strconv.ParseFloat(v, 64)
							if err != nil {
								return listRespMsg{err: err}
							}
							row = append(row, fmt.Sprintf("%v%%", percent*100))
						} else if ind == len(cols[1:])-1 {
							if enableSqlDisplay {
								row = append(row, v)
							}
						} else {
							row = append(row, v)
						}
					}
				}
			}
			rows = append(rows, row)
		}
		return listRespMsg{msgs: &QueriedRespMsg{
			Columns: columns,
			Results: rows,
		}, err: nil}
	}
}

func submitPlansData(ctx context.Context, clusterName string, nearly int,
	enableHistory bool,
	enableSqlDisplay bool,
	startTime string,
	endTime string,
	top int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		totalLatencySql, err := GenerateQueryWindowSqlElapsedTime(nearly, startTime, endTime, enableHistory)
		if err != nil {
			return listRespMsg{err: err}
		}

		_, res, err := db.GeneralQuery(ctx, totalLatencySql)
		if err != nil {
			return listRespMsg{err: err}
		}
		var totalLatency string
		if len(res) == 0 {
			return listRespMsg{err: fmt.Errorf("the database sql [%v] query time windows result not found", totalLatencySql)}
		} else {
			totalLatency = res[0]["all_latency_s"]
		}

		queries, err := GenerateTopsqlPlansQuery(nearly, top, startTime, endTime, enableHistory, totalLatency)
		if err != nil {
			return listRespMsg{err: err}
		}

		cols, res, err := db.GeneralQuery(ctx, queries)
		if err != nil {
			return listRespMsg{err: err}
		}

		columns := []string{"SQL Plans", "Elapsed Time(s)", "Executions", "Min sql Plan(s)", "Max sql Plan(s)", "Avg total keys", "Avg processed keys", `% Total SQL Time`, "SQL Digest"}

		if enableSqlDisplay {
			columns = append(columns, "SQL Text")
		}

		var rows [][]interface{}
		for _, r := range res {
			// exclude sort column name
			var row []interface{}
			for ind, c := range cols[1:] {
				for k, v := range r {
					if c == k {
						// total sql time percent
						if ind == 7 {
							percent, err := strconv.ParseFloat(v, 64)
							if err != nil {
								return listRespMsg{err: err}
							}
							row = append(row, fmt.Sprintf("%v%%", percent*100))
						} else if ind == len(cols[1:])-1 {
							if enableSqlDisplay {
								row = append(row, v)
							}
						} else {
							row = append(row, v)
						}
					}
				}
			}
			rows = append(rows, row)
		}

		return listRespMsg{msgs: &QueriedRespMsg{
			Columns: columns,
			Results: rows,
		}, err: nil}
	}
}

func submitCpuData(ctx context.Context, clusterName string, nearly int,
	startTime string,
	endTime string,
	top int, component string, concurrency int, enableSqlDisplay bool, instances []string) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		_, res, err := db.GeneralQuery(ctx, `show variables like 'tidb_enable_top_sql'`)
		if err != nil {
			return listRespMsg{err: err}
		}
		if len(res) == 0 {
			return listRespMsg{err: fmt.Errorf("the dashboard topsql feature not support, please check cluster version")}
		}

		if strings.EqualFold(res[0]["Value"], "OFF") {
			return listRespMsg{err: fmt.Errorf("the dashboard topsql feature not enabled, please [SET GLOBAL tidb_enable_top_sql = 1] enabled")}
		}

		cpus, err := GenerateTosqlCpuTimeByComponentServer(ctx, clusterName, component, nearly, top, startTime, endTime, concurrency, instances)
		if err != nil {
			return listRespMsg{err: err}
		}
		columns := []string{"CPU Time(s)", "Exec counts per sec", "Latency per exec(s)", "Scan record per sec", "Scan indexes per sec", "Plan digest counts", "Max plan sql latency(s)", "Min plan sql latency(s)", `% Total SQL Time`, "SQL Digest"}
		if enableSqlDisplay {
			columns = append(columns, "SQL Text")
		}
		var rows [][]interface{}
		for _, c := range cpus {
			var row []interface{}
			row = append(row, c.CpuTimeSec)
			row = append(row, c.ExecCountsPerSec)
			row = append(row, c.LatencyPerExecSec)
			row = append(row, c.ScanRecordPerSec)
			row = append(row, c.ScanIndexesPerSec)
			row = append(row, c.PlanDigestCounts)
			row = append(row, c.MaxPlanSqlLatencySec)
			row = append(row, c.MinPlanSqlLatencySec)

			// total sql time percent
			percent, err := strconv.ParseFloat(c.SqlLatencyPercent, 64)
			if err != nil {
				return listRespMsg{err: err}
			}
			row = append(row, fmt.Sprintf("%v%%", percent*100))
			row = append(row, c.SqlDigest)
			if enableSqlDisplay {
				row = append(row, c.SqlText)
			}
			rows = append(rows, row)
		}
		return listRespMsg{msgs: &QueriedRespMsg{
			Columns: columns,
			Results: rows,
		}, err: nil}
	}
}
