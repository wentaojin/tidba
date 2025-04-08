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
package inspect

import (
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/model"
	"gopkg.in/yaml.v3"
)

type InspectQueryModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	spinner     spinner.Model
	mode        string
	Msg         *InspectConfig
	Error       error
}

func NewInspectQueryModel(clusterName string) InspectQueryModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return InspectQueryModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
		mode:        model.BubblesModeQuering,
	}
}

func (m InspectQueryModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m InspectQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case listInspResultMsg:
		m.mode = model.BubblesModeQueried
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msg = msg.data
		}
		return m, tea.Quit
	default:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(
			cmd,
			submitListInspData(m.ctx, m.clusterName), // submit list data
		)
	}
}

func (m InspectQueryModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster inspect config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return "❌ Queried failed!\n\n"
		}
		if m.Msg.String() == "" {
			return "\n❌ Queried error: not found cluster inspect config content"
		}
		return "✅ Queried successfully!\n\n"
	}
}

type listInspResultMsg struct {
	data *InspectConfig
	err  error
}

func submitListInspData(ctx context.Context, clusterName string) tea.Cmd {

	return func() tea.Msg {
		var (
			data *InspectConfig
			err  error
		)
		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return listInspResultMsg{data: data, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		c, err := db.(*sqlite.Database).GetInspect(ctx, clusterName)
		if err != nil {
			return listInspResultMsg{data: data, err: err}
		}

		if err := yaml.Unmarshal([]byte(c.InspectConfig), &data); err != nil {
			return listInspResultMsg{data: data, err: err}
		}
		return listInspResultMsg{data: data, err: nil}
	}
}
