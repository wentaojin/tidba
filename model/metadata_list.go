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
package model

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
)

type ClusterListModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	page        uint64
	pageSize    uint64
	spinner     spinner.Model
	mode        string
	Msgs        []*sqlite.Cluster
	Error       error
}

func NewClusterListModel(clusterName string, page, pageSize uint64) ClusterListModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return ClusterListModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
		page:        page,
		pageSize:    pageSize,
	}
}

func (m ClusterListModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m ClusterListModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case listResultMsg:
		m.mode = BubblesModeQueried
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msgs = msg.datas
		}
		return m, tea.Quit
	default:
		m.mode = BubblesModeQuering
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(
			cmd,
			submitListData(m.ctx, m.clusterName, m.page, m.pageSize), // submit list data
		)
	}
}

func (m ClusterListModel) View() string {
	switch m.mode {
	case BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster config...%s",
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

type listResultMsg struct {
	datas []*sqlite.Cluster
	err   error
}

func submitListData(ctx context.Context, clusterName string, page, pageSize uint64) tea.Cmd {

	return func() tea.Msg {
		var (
			datas []*sqlite.Cluster
			err   error
		)

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return listResultMsg{datas: datas, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		if !strings.EqualFold(clusterName, "") {
			c, err := db.(*sqlite.Database).GetCluster(ctx, clusterName)
			if err != nil {
				return listResultMsg{datas: nil, err: err}
			}
			if !reflect.DeepEqual(c, &sqlite.Cluster{}) {
				datas = append(datas, c)
			} else {
				return listResultMsg{datas: datas, err: fmt.Errorf("the cluster_name [%s] not found, please retry configure cluster query -c {clusterName}", clusterName)}
			}
		} else {
			datas, err = db.(*sqlite.Database).ListCluster(ctx, page, pageSize)
			if err != nil {
				return listResultMsg{datas: nil, err: err}
			}
		}
		return listResultMsg{datas: datas, err: nil}
	}
}
