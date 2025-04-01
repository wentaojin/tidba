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
package region

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/model"
)

type RegionQueryModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	database    string
	stores      []string
	tables      []string
	indexes     []string
	hottype     string
	top         int
	command     string
	regionIds   []string
	regionType  string
	pdAddr      string
	concurrency int
	spinner     spinner.Model
	mode        string
	Msgs        interface{}
	Error       error
}

func NewRegionQueryModel(clusterName string,
	database string, stores []string, tables []string, indexes []string, hottype string, top int, command string,
	regionIds []string, regionType, pdAddr string, concurrency int) RegionQueryModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return RegionQueryModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
		database:    database,
		stores:      stores,
		tables:      tables,
		indexes:     indexes,
		hottype:     hottype,
		top:         top,
		command:     command,
		regionIds:   regionIds,
		regionType:  regionType,
		pdAddr:      pdAddr,
		concurrency: concurrency,
		mode:        model.BubblesModeQuering,
	}
}

func (m RegionQueryModel) Init() tea.Cmd {
	log.Printf("Start Work...\n")
	return m.spinner.Tick
}

func (m RegionQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	default:
		m.spinner, cmd = m.spinner.Update(msg)
		switch m.command {
		case "HOTSPOT":
			return m, tea.Batch(
				cmd,
				submitHotspotData(m.ctx, m.clusterName, m.database, m.stores, m.tables, m.indexes, m.hottype, m.top), // submit list data
			)
		case "LEADER":
			return m, tea.Batch(
				cmd,
				submitLeaderData(m.ctx, m.clusterName, m.database, m.stores, m.tables, m.indexes), // submit list data
			)
		case "REPLICA":
			return m, tea.Batch(
				cmd,
				submitDownMajorReplicaData(m.ctx, m.clusterName, m.stores, m.regionType, m.pdAddr, m.concurrency), // submit list data
			)
		case "QUERY":
			return m, tea.Batch(
				cmd,
				submitRegionData(m.ctx, m.clusterName, m.regionIds, m.pdAddr, m.concurrency), // submit list data
			)
		default:
			return m, tea.Quit
		}

	}
}

func (m RegionQueryModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster region information...%s",
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
	Results []map[string]string
}

func submitHotspotData(ctx context.Context, clusterName string, dbName string, stores []string, tables []string, indexes []string, hottype string, top int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		queries, err := GenerateHotspotRegionQuerySql(clusterName, dbName, stores, tables, indexes, hottype, top)
		if err != nil {
			return listRespMsg{err: err}
		}
		var msgs []*QueriedRespMsg
		for _, query := range queries {
			cols, res, err := db.GeneralQuery(ctx, query)
			if err != nil {
				return listRespMsg{err: err}
			}
			msgs = append(msgs, &QueriedRespMsg{
				Columns: cols,
				Results: res,
			})
		}
		return listRespMsg{msgs: msgs, err: nil}
	}
}

func submitLeaderData(ctx context.Context, clusterName string, dbName string, stores []string, tables []string, indexes []string) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		queries, err := GenerateLeaderDistributedQuerySql(clusterName, dbName, stores, tables, indexes)
		if err != nil {
			return listRespMsg{err: err}
		}
		var msgs []*QueriedRespMsg
		for _, query := range queries {
			cols, res, err := db.GeneralQuery(ctx, query)
			if err != nil {
				return listRespMsg{err: err}
			}
			msgs = append(msgs, &QueriedRespMsg{
				Columns: cols,
				Results: res,
			})
		}
		return listRespMsg{msgs: msgs, err: nil}
	}
}

func submitRegionData(ctx context.Context, clusterName string, regionIds []string, pdAddr string, concurrency int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		regions, err := QueryRegionIDInformation(ctx, clusterName, db, regionIds, pdAddr, concurrency)
		if err != nil {
			return listRespMsg{err: err}
		}

		return listRespMsg{msgs: regions, err: nil}
	}
}

func submitDownMajorReplicaData(ctx context.Context, clusterName string, storeAddrs []string, regionType, pdAddr string, concurrency int) tea.Cmd {
	return func() tea.Msg {
		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		db := connDB.(*mysql.Database)

		regions, err := QueryMarjorDownRegionPeers(ctx, clusterName, db, pdAddr, regionType, concurrency, storeAddrs)
		if err != nil {
			return listRespMsg{err: err}
		}

		return listRespMsg{msgs: regions, err: nil}
	}
}
