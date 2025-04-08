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

type InspectDeleteModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	spinner     spinner.Model
	mode        string
	Msg         string
	Error       error
}

func NewInspectDeleteModel(clusterName string) InspectDeleteModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return InspectDeleteModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
	}
}

func (m InspectDeleteModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m InspectDeleteModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case delInspResultMsg:
		m.mode = model.BubblesModeDeleted
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msg = msg.data
		}
		return m, tea.Quit
	default:
		m.mode = model.BubblesModeDeleting
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(
			cmd,
			submitDelInspData(m.ctx, m.clusterName), // submit delete data
		)
	}
}

func (m InspectDeleteModel) View() string {
	switch m.mode {
	case model.BubblesModeDeleting:
		return fmt.Sprintf(
			"%s Deleting cluster ispection config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return "\n❌ Deleted failed!\n\n"
		}
		return "✅ Deleted successfully!\n\n"
	}
}

type delInspResultMsg struct {
	data string
	err  error
}

func submitDelInspData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return delInspResultMsg{data: "", err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		c, err := db.(*sqlite.Database).DeleteInspect(ctx, clusterName)
		if err != nil {
			return delInspResultMsg{data: "", err: err}
		}

		var insp *InspectConfig
		if err := yaml.Unmarshal([]byte(c.InspectConfig), &insp); err != nil {
			return delInspResultMsg{data: "", err: err}
		}
		return delInspResultMsg{data: insp.String(), err: nil}
	}
}
