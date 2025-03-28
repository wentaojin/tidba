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

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
)

type clusterDeleteModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	spinner     spinner.Model
	mode        string
	delMsg      []*sqlite.Cluster
	delError    string
}

func NewClusterDeleteModel(clusterName string) clusterDeleteModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return clusterDeleteModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
	}
}

func (m clusterDeleteModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m clusterDeleteModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case delResultMsg:
		m.mode = BubblesModeDeleted
		if msg.err != nil {
			m.delError = msg.err.Error()
		} else {
			m.delMsg = msg.datas
		}
		return m, tea.Quit
	default:
		m.mode = BubblesModeDeleting
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(
			cmd,
			submitDelData(m.ctx, m.clusterName), // submit delete data
		)
	}
}

func (m clusterDeleteModel) View() string {
	switch m.mode {
	case BubblesModeDeleting:
		return fmt.Sprintf(
			"%s Deleting cluster config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.delError != "" {
			return fmt.Sprintf("\n❌ Deleted error: %s\n", m.delError)
		}

		t := table.NewWriter()
		t.AppendHeader(table.Row{"cluster_name", "database", "path", "private_key"})
		t.AppendSeparator()
		for _, c := range m.delMsg {
			t.AppendRow(table.Row{
				c.ClusterName,
				fmt.Sprintf("%s[%s]@%s:%d", c.DbUser, c.DbPassword, c.DbHost, c.DbPort),
				c.Path,
				c.PrivateKey})
		}
		return fmt.Sprintf(
			"✅ Deleted successfully!\n\ncluster config content:\n%s\n\n",
			t.Render(),
		)
	}
}

type delResultMsg struct {
	datas []*sqlite.Cluster
	err   error
}

func submitDelData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		var datas []*sqlite.Cluster

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return delResultMsg{datas: datas, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		c, err := db.(*sqlite.Database).DeleteCluster(ctx, clusterName)
		if err != nil {
			return delResultMsg{datas: datas, err: err}
		}
		if !reflect.DeepEqual(c, &sqlite.Cluster{}) {
			datas = append(datas, c)
			return delResultMsg{datas: datas, err: nil}
		} else {
			return delResultMsg{datas: datas, err: fmt.Errorf("the cluster_name [%s] not found, please retry configure cluster query -c {clusterName}", clusterName)}
		}
	}
}
